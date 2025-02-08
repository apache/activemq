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
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.apache.activemq.store.MessageRecoveryContext;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.protobuf.UTF8Buffer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StoreBackup {

    private static final Logger logger = LoggerFactory.getLogger(StoreBackup.class);

    static final int OPENWIRE_VERSION = 11;
    static final boolean TIGHT_ENCODING = false;

    URI config;
    String filename;
    File file;

    String queue;
    Integer offset;
    Integer count = Integer.MAX_VALUE;
    String indexes;
    Collection<Integer> indexesList;

    String startMsgId;
    String endMsgId;

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
            throw new IllegalArgumentException("required --config option missing");
        }
        if (filename == null) {
            throw new IllegalArgumentException("required --filename option missing");
        }

        if (offset != null && count == null) {
            throw new IllegalArgumentException("optional --offset and --count must be specified together");
        }

        if ((startMsgId != null || endMsgId != null) && queue == null) {
            throw new IllegalArgumentException("optional --queue must be specified when using startMsgId or endMsgId");
        }

        if (indexes != null && !indexes.isBlank()) {
            indexesList = Stream.of(indexes.split(","))
                .map(index -> Integer.parseInt(index.trim()))
                .peek(num -> {
                    if (num < 0) {
                        throw new IllegalArgumentException("Index value cannot be negative: " + num);
                    }
                }).collect(Collectors.toList());
        }

        setFile(new File(filename));
        logger.info("Loading config file:{} ", config);
        BrokerFactory.setStartDefault(false); // to avoid the broker auto-starting..
        BrokerService broker = BrokerFactory.createBroker(config);
        BrokerFactory.resetStartDefault();
        PersistenceAdapter store = broker.getPersistenceAdapter();

        logger.info("Starting: " + store);
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
            throw new IllegalStateException("Cannot export a store with prepared XA transactions.  Please commit or rollback those transactions before attempting to backup.");
        }

        for (ActiveMQDestination odest : store.getDestinations()) {

            if(queue != null && !queue.equals(odest.getPhysicalName())) {
                continue;
            }

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
                logger.info("Queue info:{}", json);
                destRecord.setBindingData(new UTF8Buffer(json));
                manager.store_queue(destRecord);

                MessageRecoveryContext.Builder builder = 
                        new MessageRecoveryContext.Builder()
                            .maxMessageCountReturned(count)
                            .messageRecoveryListener(new MessageRecoveryListener() {

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

                if(startMsgId != null || endMsgId != null) {
                    logger.info("Backing up from startMsgId:{} to endMsgId:{} ", startMsgId, endMsgId);
                    queue.recoverMessages(builder.endMessageId(endMsgId).startMessageId(startMsgId).build());
                } else if(indexesList != null) {
                    logger.info("Backing up using indexes count:{}", indexesList.size());
                    for(int idx : indexesList) {
                        queue.recoverMessages(builder.maxMessageCountReturned(1).offset(idx).build());
                    }
                } else if(offset != null) {
                    logger.info("Backing up from offset:{} count:{} ", offset, count);
                    queue.recoverMessages(builder.offset(offset).build());
                } else {
                    queue.recover(builder.build());
                }
            } else if (odest instanceof ActiveMQTopic) {
                ActiveMQTopic dest = (ActiveMQTopic) odest;

                TopicMessageStore topic = store.createTopicMessageStore(dest);
                for (SubscriptionInfo sub : topic.getAllSubscriptions()) {

                    QueuePB destRecord = new QueuePB();
                    destRecord.setKey(containerKeyCounter[0]);
                    destRecord.setBindingKind(ds_kind);

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
                    logger.info("Topic info:{}", json);

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
        mos.writeInt(OPENWIRE_VERSION);
        wireformat.marshal(message, mos);

        MessagePB messageRecord = new MessagePB();
        messageRecord.setCodec(codec_id);
        messageRecord.setMessageKey(messageKey);
        messageRecord.setSize(message.getSize());
        messageRecord.setValue(new Buffer(mos.getData()));
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

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getQueue() {
        return queue;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Integer getCount() {
        return count;
    }

    public void setIndexes(String indexes) {
        this.indexes = indexes;
    }

    public String getIndexes() {
        return indexes;
    }

    public String getStartMsgId() {
        return startMsgId;
    }

    public void setStartMsgId(String startMsgId) {
        this.startMsgId = startMsgId;
    }

    public String getEndMsgId() {
        return endMsgId;
    }

    public void setEndMsgId(String endMsgId) {
        this.endMsgId = endMsgId;
    }
}
