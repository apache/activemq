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

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.BitArrayBin;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.LRUCache;

/**
 * Provides basic audit functions for Messages without sync
 * 
 * 
 */
public class ActiveMQMessageAuditNoSync implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_WINDOW_SIZE = 2048;
    public static final int MAXIMUM_PRODUCER_COUNT = 64;
    private int auditDepth;
    private int maximumNumberOfProducersToTrack;
    private LRUCache<Object, BitArrayBin> map;

    /**
     * Default Constructor windowSize = 2048, maximumNumberOfProducersToTrack =
     * 64
     */
    public ActiveMQMessageAuditNoSync() {
        this(DEFAULT_WINDOW_SIZE, MAXIMUM_PRODUCER_COUNT);
    }

    /**
     * Construct a MessageAudit
     * 
     * @param auditDepth range of ids to track
     * @param maximumNumberOfProducersToTrack number of producers expected in
     *                the system
     */
    public ActiveMQMessageAuditNoSync(int auditDepth, final int maximumNumberOfProducersToTrack) {
        this.auditDepth = auditDepth;
        this.maximumNumberOfProducersToTrack=maximumNumberOfProducersToTrack;
        this.map = new LRUCache<Object, BitArrayBin>(0, maximumNumberOfProducersToTrack, 0.75f, true);
    }
    
    /**
     * @return the auditDepth
     */
    public int getAuditDepth() {
        return auditDepth;
    }

    /**
     * @param auditDepth the auditDepth to set
     */
    public void setAuditDepth(int auditDepth) {
        this.auditDepth = auditDepth;
    }

    /**
     * @return the maximumNumberOfProducersToTrack
     */
    public int getMaximumNumberOfProducersToTrack() {
        return maximumNumberOfProducersToTrack;
    }

    /**
     * @param maximumNumberOfProducersToTrack the maximumNumberOfProducersToTrack to set
     */
    public void setMaximumNumberOfProducersToTrack(
            int maximumNumberOfProducersToTrack) {
        this.maximumNumberOfProducersToTrack = maximumNumberOfProducersToTrack;
        this.map.setMaxCacheSize(maximumNumberOfProducersToTrack);
    }

    /**
     * Checks if this message has been seen before
     * 
     * @param message
     * @return true if the message is a duplicate
     * @throws JMSException
     */
    public boolean isDuplicate(Message message) throws JMSException {
        return isDuplicate(message.getJMSMessageID());
    }

    /**
     * checks whether this messageId has been seen before and adds this
     * messageId to the list
     * 
     * @param id
     * @return true if the message is a duplicate
     */
    public boolean isDuplicate(String id) {
        boolean answer = false;
        String seed = IdGenerator.getSeedFromId(id);
        if (seed != null) {
            BitArrayBin bab = map.get(seed);
            if (bab == null) {
                bab = new BitArrayBin(auditDepth);
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
    public boolean isDuplicate(final MessageReference message) {
        MessageId id = message.getMessageId();
        return isDuplicate(id);
    }
    
    /**
     * Checks if this messageId has been seen before
     * 
     * @param id
     * @return true if the message is a duplicate
     */
    public boolean isDuplicate(final MessageId id) {
        boolean answer = false;
        
        if (id != null) {
            ProducerId pid = id.getProducerId();
            if (pid != null) {
                BitArrayBin bab = map.get(pid);
                if (bab == null) {
                    bab = new BitArrayBin(auditDepth);
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
    public void rollback(final MessageReference message) {
        MessageId id = message.getMessageId();
        rollback(id);
    }
    
    /**
     * mark this message as being received
     * 
     * @param id
     */
    public void rollback(final  MessageId id) {
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

    public void rollback(final String id) {
        String seed = IdGenerator.getSeedFromId(id);
        if (seed != null) {
            BitArrayBin bab = map.get(seed);
            if (bab != null) {
                long index = IdGenerator.getSequenceFromId(id);
                bab.setBit(index, false);
            }
        }
    }
    
    /**
     * Check the message is in order
     * @param msg
     * @return
     * @throws JMSException
     */
    public boolean isInOrder(Message msg) throws JMSException {
        return isInOrder(msg.getJMSMessageID());
    }
    
    /**
     * Check the message id is in order
     * @param id
     * @return
     */
    public boolean isInOrder(final String id) {
        boolean answer = true;
        
        if (id != null) {
            String seed = IdGenerator.getSeedFromId(id);
            if (seed != null) {
                BitArrayBin bab = map.get(seed);
                if (bab != null) {
                    long index = IdGenerator.getSequenceFromId(id);
                    answer = bab.isInOrder(index);
                }
               
            }
        }
        return answer;
    }
    
    /**
     * Check the MessageId is in order
     * @param message 
     * @return
     */
    public boolean isInOrder(final MessageReference message) {
        return isInOrder(message.getMessageId());
    }
    
    /**
     * Check the MessageId is in order
     * @param id
     * @return
     */
    public boolean isInOrder(final MessageId id) {
        boolean answer = false;

        if (id != null) {
            ProducerId pid = id.getProducerId();
            if (pid != null) {
                BitArrayBin bab = map.get(pid);
                if (bab == null) {
                    bab = new BitArrayBin(auditDepth);
                    map.put(pid, bab);
                }
                answer = bab.isInOrder(id.getProducerSequenceId());

            }
        }
        return answer;
    }

    public long getLastSeqId(ProducerId id) {
        long result = -1;
        BitArrayBin bab = map.get(id.toString() + ":");
        if (bab != null) {
            result = bab.getLastSetIndex();
        }
        return result;
    }

    public void clear() {
        map.clear();
    }
}
