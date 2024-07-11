/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.console.command.store;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.console.command.store.protobuf.MessagePB;
import org.apache.activemq.console.command.store.protobuf.QueueEntryPB;
import org.apache.activemq.console.command.store.protobuf.QueuePB;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.protobuf.UTF8Buffer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Matt Pavlovich <mattrpav@apache.org>
 */
public class StoreBackup {

    static final int OPENWIRE_VERSION = 11;
    static final boolean TIGHT_ENCODING = false;

    URI config;
    String filename;
    File file;

    private final ObjectMapper mapper = new ObjectMapper();
    private final AsciiBuffer ds_kind = new AsciiBuffer("ds");
    private final AsciiBuffer ptp_kind = new AsciiBuffer("ptp");
    private final AsciiBuffer codec_id = new AsciiBuffer("openwire");
    private final OpenWireFormat wireformat = new OpenWireFormat();

    public StoreBackup() throws URISyntaxException {
        config = new URI("xbean:activemq.xml");
        wireformat.setCacheEnabled(false);
        wireformat.setTightEncodingEnabled(TIGHT_ENCODING);
        wireformat.setVersion(OPENWIRE_VERSION);
    }

    public void execute() throws Exception {
        if (config == null) {
            throw new Exception("required --config option missing");
        }
        if (filename == null) {
            throw new Exception("required --filename option missing");
        }

        setFile(new File(filename));
        System.out.println("Loading: " + config);
        BrokerFactory.setStartDefault(false); // to avoid the broker auto-starting..
        BrokerService broker = BrokerFactory.createBroker(config);
        BrokerFactory.resetStartDefault();
        PersistenceAdapter store = broker.getPersistenceAdapter();

        System.out.println("Starting: " + store);
        store.start();
        try(BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(file))) {
            export(store, fos);
        } finally {
            store.stop();
        }
    }

    void export(PersistenceAdapter store, BufferedOutputStream fos) throws Exception {

        final long[] messageKeyCounter = new long[]{0};
        final long[] containerKeyCounter = new long[]{0};
        final BackupStreamManager manager = new BackupStreamManager(fos, 1);

        final int[] preparedTxs = new int[]{0};
        store.createTransactionStore().recover(new TransactionRecoveryListener() {
            @Override
            public void recover(XATransactionId xid, Message[] addedMessages, MessageAck[] aks) {
                preparedTxs[0] += 1;
            }
        });

        if (preparedTxs[0] > 0) {
            throw new Exception("Cannot export a store with prepared XA transactions.  Please commit or rollback those transactions before attempting to backup.");
        }

        for (ActiveMQDestination odest : store.getDestinations()) {
            containerKeyCounter[0]++;
            if (odest instanceof ActiveMQQueue) {
                ActiveMQQueue dest = (ActiveMQQueue) odest;
                MessageStore queue = store.createQueueMessageStore(dest);

                QueuePB destRecord = new QueuePB();
                destRecord.setKey(containerKeyCounter[0]);
                destRecord.setBindingKind(ptp_kind);

                final long[] seqKeyCounter = new long[]{0};

                HashMap<String, Object> jsonMap = new HashMap<String, Object>();
                jsonMap.put("@class", "queue_destination");
                jsonMap.put("name", dest.getQueueName());
                String json = mapper.writeValueAsString(jsonMap);
                System.out.println(json);
                destRecord.setBindingData(new UTF8Buffer(json));
                manager.store_queue(destRecord);

                queue.recover(new MessageRecoveryListener() {
                    @Override
                    public boolean hasSpace() {
                        return true;
                    }

                    @Override
                    public boolean recoverMessageReference(MessageId ref) throws Exception {
                        return true;
                    }

                    @Override
                    public boolean isDuplicate(MessageId ref) {
                        return false;
                    }

                    @Override
                    public boolean recoverMessage(Message message) throws IOException {
                        messageKeyCounter[0]++;
                        seqKeyCounter[0]++;

                        MessagePB messageRecord = createMessagePB(message, messageKeyCounter[0]);
                        manager.store_message(messageRecord);

                        QueueEntryPB entryRecord = createQueueEntryPB(message, containerKeyCounter[0], seqKeyCounter[0], messageKeyCounter[0]);
                        manager.store_queue_entry(entryRecord);

                        return true;
                    }
                });

            } else if (odest instanceof ActiveMQTopic) {
                ActiveMQTopic dest = (ActiveMQTopic) odest;

                TopicMessageStore topic = store.createTopicMessageStore(dest);
                for (SubscriptionInfo sub : topic.getAllSubscriptions()) {

                    QueuePB destRecord = new QueuePB();
                    destRecord.setKey(containerKeyCounter[0]);
                    destRecord.setBindingKind(ds_kind);

                    // TODO: use a real JSON encoder like jackson.
                    HashMap<String, Object> jsonMap = new HashMap<String, Object>();
                    jsonMap.put("@class", "dsub_destination");
                    jsonMap.put("name", sub.getClientId() + ":" + sub.getSubscriptionName());
                    HashMap<String, Object> jsonTopic = new HashMap<String, Object>();
                    jsonTopic.put("name", dest.getTopicName());
                    jsonMap.put("topics", new Object[]{jsonTopic});
                    if (sub.getSelector() != null) {
                        jsonMap.put("selector", sub.getSelector());
                    }
                    jsonMap.put("noLocal", sub.isNoLocal());
                    String json = mapper.writeValueAsString(jsonMap);
                    System.out.println(json);

                    destRecord.setBindingData(new UTF8Buffer(json));
                    manager.store_queue(destRecord);

                    final long seqKeyCounter[] = new long[]{0};
                    topic.recoverSubscription(sub.getClientId(), sub.getSubscriptionName(), new MessageRecoveryListener() {
                        @Override
                        public boolean hasSpace() {
                            return true;
                        }

                        @Override
                        public boolean recoverMessageReference(MessageId ref) throws Exception {
                            return true;
                        }

                        @Override
                        public boolean isDuplicate(MessageId ref) {
                            return false;
                        }

                        @Override
                        public boolean recoverMessage(Message message) throws IOException {
                            messageKeyCounter[0]++;
                            seqKeyCounter[0]++;

                            MessagePB messageRecord = createMessagePB(message, messageKeyCounter[0]);
                            manager.store_message(messageRecord);

                            QueueEntryPB entryRecord = createQueueEntryPB(message, containerKeyCounter[0], seqKeyCounter[0], messageKeyCounter[0]);
                            manager.store_queue_entry(entryRecord);
                            return true;
                        }
                    });

                }
            }
        }
        manager.finish();
    }

    private QueueEntryPB createQueueEntryPB(Message message, long queueKey, long queueSeq, long messageKey) {
        QueueEntryPB entryRecord = new QueueEntryPB();
        entryRecord.setQueueKey(queueKey);
        entryRecord.setQueueSeq(queueSeq);
        entryRecord.setMessageKey(messageKey);
        entryRecord.setSize(message.getSize());
        if (message.getExpiration() != 0) {
            entryRecord.setExpiration(message.getExpiration());
        }
        if (message.getRedeliveryCounter() != 0) {
            entryRecord.setRedeliveries(message.getRedeliveryCounter());
        }
        return entryRecord;
    }

    private MessagePB createMessagePB(Message message, long messageKey) throws IOException {
        DataByteArrayOutputStream mos = new DataByteArrayOutputStream();
        mos.writeBoolean(TIGHT_ENCODING);
        mos.writeVarInt(OPENWIRE_VERSION);
        wireformat.marshal(message, mos);

        MessagePB messageRecord = new MessagePB();
        messageRecord.setCodec(codec_id);
        messageRecord.setMessageKey(messageKey);
        messageRecord.setSize(message.getSize());
        messageRecord.setValue(new Buffer(mos.toBuffer().getData()));
        return messageRecord;
    }

    public File getFile() {
        return file;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }
    
    public void setFile(File file) {
        this.file = file;
    }

    public URI getConfig() {
        return config;
    }

    public void setConfig(URI config) {
        this.config = config;
    }
}
