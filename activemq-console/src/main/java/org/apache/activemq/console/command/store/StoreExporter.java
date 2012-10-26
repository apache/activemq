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

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.*;
import org.apache.activemq.console.command.store.proto.MessagePB;
import org.apache.activemq.console.command.store.proto.QueueEntryPB;
import org.apache.activemq.console.command.store.proto.QueuePB;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.store.*;
import org.apache.activemq.xbean.XBeanBrokerFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtbuf.UTF8Buffer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class StoreExporter {

    URI config;
    File file;

    public StoreExporter() throws URISyntaxException {
        config = new URI("xbean:activemq.xml");
    }

    public void execute() throws Exception {
        if (config == null) {
            throw new Exception("required --config option missing");
        }
        if (file == null) {
            throw new Exception("required --file option missing");
        }
        System.out.println("Loading: " + config);
        XBeanBrokerFactory.setStartDefault(false); // to avoid the broker auto-starting..
        BrokerService broker = BrokerFactory.createBroker(config);
        XBeanBrokerFactory.resetStartDefault();
        PersistenceAdapter store = broker.getPersistenceAdapter();
        System.out.println("Starting: " + store);
        store.start();
        try {
            BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(file));
            try {
                export(store, fos);
            } finally {
                fos.close();
            }
        } finally {
            store.stop();
        }
    }

    static final int OPENWIRE_VERSION = 8;
    static final boolean TIGHT_ENCODING = false;

    void export(PersistenceAdapter store, BufferedOutputStream fos) throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        final AsciiBuffer ds_kind = new AsciiBuffer("ds");
        final AsciiBuffer ptp_kind = new AsciiBuffer("ptp");
        final AsciiBuffer codec_id = new AsciiBuffer("openwire");
        final OpenWireFormat wireformat = new OpenWireFormat();
        wireformat.setCacheEnabled(false);
        wireformat.setTightEncodingEnabled(TIGHT_ENCODING);
        wireformat.setVersion(OPENWIRE_VERSION);

        final long[] messageKeyCounter = new long[]{0};
        final long[] containerKeyCounter = new long[]{0};
        final ExportStreamManager manager = new ExportStreamManager(fos, 1);


        final int[] preparedTxs = new int[]{0};
        store.createTransactionStore().recover(new TransactionRecoveryListener() {
            public void recover(XATransactionId xid, Message[] addedMessages, MessageAck[] aks) {
                preparedTxs[0] += 1;
            }
        });

        if (preparedTxs[0] > 0) {
            throw new Exception("Cannot export a store with prepared XA transactions.  Please commit or rollback those transactions before attempting to export.");
        }

        for (ActiveMQDestination odest : store.getDestinations()) {
            containerKeyCounter[0]++;
            if (odest instanceof ActiveMQQueue) {
                ActiveMQQueue dest = (ActiveMQQueue) odest;
                MessageStore queue = store.createQueueMessageStore(dest);

                QueuePB.Bean destRecord = new QueuePB.Bean();
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
                    public boolean hasSpace() {
                        return true;
                    }

                    public boolean recoverMessageReference(MessageId ref) throws Exception {
                        return true;
                    }

                    public boolean isDuplicate(MessageId ref) {
                        return false;
                    }

                    public boolean recoverMessage(Message message) throws IOException {
                        messageKeyCounter[0]++;
                        seqKeyCounter[0]++;

                        DataByteArrayOutputStream mos = new DataByteArrayOutputStream();
                        mos.writeBoolean(TIGHT_ENCODING);
                        mos.writeVarInt(OPENWIRE_VERSION);
                        wireformat.marshal(message, mos);

                        MessagePB.Bean messageRecord = new MessagePB.Bean();
                        messageRecord.setCodec(codec_id);
                        messageRecord.setMessageKey(messageKeyCounter[0]);
                        messageRecord.setSize(message.getSize());
                        messageRecord.setValue(mos.toBuffer());
                        //                record.setCompression()
                        manager.store_message(messageRecord);

                        QueueEntryPB.Bean entryRecord = new QueueEntryPB.Bean();
                        entryRecord.setQueueKey(containerKeyCounter[0]);
                        entryRecord.setQueueSeq(seqKeyCounter[0]);
                        entryRecord.setMessageKey(messageKeyCounter[0]);
                        entryRecord.setSize(message.getSize());
                        if (message.getExpiration() != 0) {
                            entryRecord.setExpiration(message.getExpiration());
                        }
                        if (message.getRedeliveryCounter() != 0) {
                            entryRecord.setRedeliveries(message.getRedeliveryCounter());
                        }
                        manager.store_queue_entry(entryRecord);
                        return true;
                    }
                });

            } else if (odest instanceof ActiveMQTopic) {
                ActiveMQTopic dest = (ActiveMQTopic) odest;

                TopicMessageStore topic = store.createTopicMessageStore(dest);
                for (SubscriptionInfo sub : topic.getAllSubscriptions()) {

                    QueuePB.Bean destRecord = new QueuePB.Bean();
                    destRecord.setKey(containerKeyCounter[0]);
                    destRecord.setBindingKind(ds_kind);

                    // TODO: use a real JSON encoder like jackson.
                    HashMap<String, Object> jsonMap = new HashMap<String, Object>();
                    jsonMap.put("@class", "dsub_destination");
                    jsonMap.put("name", sub.getClientId() + ":" + sub.getSubcriptionName());
                    HashMap<String, Object> jsonTopic = new HashMap<String, Object>();
                    jsonTopic.put("name", dest.getTopicName());
                    jsonMap.put("topics", new Object[]{jsonTopic});
                    if (sub.getSelector() != null) {
                        jsonMap.put("selector", sub.getSelector());
                    }
                    String json = mapper.writeValueAsString(jsonMap);
                    System.out.println(json);

                    destRecord.setBindingData(new UTF8Buffer(json));
                    manager.store_queue(destRecord);

                    final long seqKeyCounter[] = new long[]{0};
                    topic.recoverSubscription(sub.getClientId(), sub.getSubcriptionName(), new MessageRecoveryListener() {
                        public boolean hasSpace() {
                            return true;
                        }

                        public boolean recoverMessageReference(MessageId ref) throws Exception {
                            return true;
                        }

                        public boolean isDuplicate(MessageId ref) {
                            return false;
                        }

                        public boolean recoverMessage(Message message) throws IOException {
                            messageKeyCounter[0]++;
                            seqKeyCounter[0]++;

                            DataByteArrayOutputStream mos = new DataByteArrayOutputStream();
                            mos.writeBoolean(TIGHT_ENCODING);
                            mos.writeVarInt(OPENWIRE_VERSION);
                            wireformat.marshal(mos);

                            MessagePB.Bean messageRecord = new MessagePB.Bean();
                            messageRecord.setCodec(codec_id);
                            messageRecord.setMessageKey(messageKeyCounter[0]);
                            messageRecord.setSize(message.getSize());
                            messageRecord.setValue(mos.toBuffer());
                            //                record.setCompression()
                            manager.store_message(messageRecord);

                            QueueEntryPB.Bean entryRecord = new QueueEntryPB.Bean();
                            entryRecord.setQueueKey(containerKeyCounter[0]);
                            entryRecord.setQueueSeq(seqKeyCounter[0]);
                            entryRecord.setMessageKey(messageKeyCounter[0]);
                            entryRecord.setSize(message.getSize());
                            if (message.getExpiration() != 0) {
                                entryRecord.setExpiration(message.getExpiration());
                            }
                            if (message.getRedeliveryCounter() != 0) {
                                entryRecord.setRedeliveries(message.getRedeliveryCounter());
                            }
                            manager.store_queue_entry(entryRecord);
                            return true;
                        }
                    });

                }
            }
        }
        manager.finish();
    }

    public File getFile() {
        return file;
    }

    public void setFile(String file) {
        setFile(new File(file));
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
