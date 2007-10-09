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
package org.apache.activemq;

import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.BitArrayBin;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LRUCache;

/**
 * Provides basic audit functions for Messages
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class ActiveMQMessageAudit {

    private static final int DEFAULT_WINDOW_SIZE = 1024;
    private static final int MAXIMUM_PRODUCER_COUNT = 128;
    private int windowSize;
    private Map<Object, BitArrayBin> map;

    /**
     * Default Constructor windowSize = 1024, maximumNumberOfProducersToTrack =
     * 128
     */
    public ActiveMQMessageAudit() {
        this(DEFAULT_WINDOW_SIZE, MAXIMUM_PRODUCER_COUNT);
    }

    /**
     * Construct a MessageAudit
     * 
     * @param windowSize range of ids to track
     * @param maximumNumberOfProducersToTrack number of producers expected in
     *                the system
     */
    public ActiveMQMessageAudit(int windowSize, final int maximumNumberOfProducersToTrack) {
        this.windowSize = windowSize;
        map = new LRUCache<Object, BitArrayBin>(maximumNumberOfProducersToTrack, maximumNumberOfProducersToTrack, 0.75f, true);
    }

    /**
     * Checks if this message has beeb seen before
     * 
     * @param message
     * @return true if the message is a duplicate
     * @throws JMSException
     */
    public boolean isDuplicateMessage(Message message) throws JMSException {
        return isDuplicate(message.getJMSMessageID());
    }

    /**
     * checks whether this messageId has been seen before and adds this
     * messageId to the list
     * 
     * @param id
     * @return true if the message is a duplicate
     */
    public synchronized boolean isDuplicate(String id) {
        boolean answer = false;
        String seed = IdGenerator.getSeedFromId(id);
        if (seed != null) {
            BitArrayBin bab = map.get(seed);
            if (bab == null) {
                bab = new BitArrayBin(windowSize);
                map.put(seed, bab);
            }
            long index = IdGenerator.getSequenceFromId(id);
            if (index >= 0) {
                answer = bab.setBit(index, true);
            }
        }
        return answer;
    }

    /**
     * Checks if this message has been seen before
     * 
     * @param message
     * @return true if the message is a duplicate
     */
    public boolean isDuplicateMessageReference(final MessageReference message) {
        MessageId id = message.getMessageId();
        return isDuplicateMessageId(id);
    }
    
    /**
     * Checks if this messageId has been seen before
     * 
     * @param id
     * @return true if the message is a duplicate
     */
    public synchronized boolean isDuplicateMessageId(final MessageId id) {
        boolean answer = false;
        
        if (id != null) {
            ProducerId pid = id.getProducerId();
            if (pid != null) {
                BitArrayBin bab = map.get(pid);
                if (bab == null) {
                    bab = new BitArrayBin(windowSize);
                    map.put(pid, bab);
                }
                answer = bab.setBit(id.getProducerSequenceId(), true);
            }
        }
        return answer;
    }

    /**
     * mark this message as being received
     * 
     * @param message
     */
    public void rollbackMessageReference(final MessageReference message) {
        MessageId id = message.getMessageId();
        rollbackMessageId(id);
    }
    
    /**
     * mark this message as being received
     * 
     * @param id
     */
    public synchronized void rollbackMessageId(final  MessageId id) {
        if (id != null) {
            ProducerId pid = id.getProducerId();
            if (pid != null) {
                BitArrayBin bab = map.get(pid);
                if (bab != null) {
                    bab.setBit(id.getProducerSequenceId(), false);
                }
            }
        }
    }
}
