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
package org.apache.activemq.broker;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.state.ProducerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds internal state in the broker for a MessageProducer
 */
public class ProducerBrokerExchange {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerBrokerExchange.class);
    private ConnectionContext connectionContext;
    private Destination regionDestination;
    private Region region;
    private ProducerState producerState;
    private boolean mutable = true;
    private AtomicLong lastSendSequenceNumber = new AtomicLong(-1);
    private boolean auditProducerSequenceIds;
    private boolean isNetworkProducer;
    private BrokerService brokerService;
    private FlowControlInfo flowControlInfo = new FlowControlInfo();

    public ProducerBrokerExchange() {
    }

    public ProducerBrokerExchange copy() {
        ProducerBrokerExchange rc = new ProducerBrokerExchange();
        rc.connectionContext = connectionContext.copy();
        rc.regionDestination = regionDestination;
        rc.region = region;
        rc.producerState = producerState;
        rc.mutable = mutable;
        rc.flowControlInfo = flowControlInfo;
        return rc;
    }


    /**
     * @return the connectionContext
     */
    public ConnectionContext getConnectionContext() {
        return this.connectionContext;
    }

    /**
     * @param connectionContext the connectionContext to set
     */
    public void setConnectionContext(ConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
    }

    /**
     * @return the mutable
     */
    public boolean isMutable() {
        return this.mutable;
    }

    /**
     * @param mutable the mutable to set
     */
    public void setMutable(boolean mutable) {
        this.mutable = mutable;
    }

    /**
     * @return the regionDestination
     */
    public Destination getRegionDestination() {
        return this.regionDestination;
    }

    /**
     * @param regionDestination the regionDestination to set
     */
    public void setRegionDestination(Destination regionDestination) {
        this.regionDestination = regionDestination;
    }

    /**
     * @return the region
     */
    public Region getRegion() {
        return this.region;
    }

    /**
     * @param region the region to set
     */
    public void setRegion(Region region) {
        this.region = region;
    }

    /**
     * @return the producerState
     */
    public ProducerState getProducerState() {
        return this.producerState;
    }

    /**
     * @param producerState the producerState to set
     */
    public void setProducerState(ProducerState producerState) {
        this.producerState = producerState;
    }

    /**
     * Enforce duplicate suppression using info from persistence adapter
     *
     * @return false if message should be ignored as a duplicate
     */
    public boolean canDispatch(Message messageSend) {
        boolean canDispatch = true;
        if (auditProducerSequenceIds && messageSend.isPersistent()) {
            final long producerSequenceId = messageSend.getMessageId().getProducerSequenceId();
            if (isNetworkProducer) {
                //  messages are multiplexed on this producer so we need to query the persistenceAdapter
                long lastStoredForMessageProducer = getStoredSequenceIdForMessage(messageSend.getMessageId());
                if (producerSequenceId <= lastStoredForMessageProducer) {
                    canDispatch = false;
                    LOG.warn("suppressing duplicate message send [{}] from network producer with producerSequence [{}] less than last stored: {}", new Object[]{
                            (LOG.isTraceEnabled() ? messageSend : messageSend.getMessageId()), producerSequenceId, lastStoredForMessageProducer
                    });
                }
            } else if (producerSequenceId <= lastSendSequenceNumber.get()) {
                canDispatch = false;
                if (messageSend.isInTransaction()) {
                    LOG.warn("suppressing duplicated message send [{}] with producerSequenceId [{}] <= last stored: {}", new Object[]{
                            (LOG.isTraceEnabled() ? messageSend : messageSend.getMessageId()), producerSequenceId, lastSendSequenceNumber
                    });
                } else {
                    LOG.debug("suppressing duplicated message send [{}] with producerSequenceId [{}] <= last stored: {}", new Object[]{
                            (LOG.isTraceEnabled() ? messageSend : messageSend.getMessageId()), producerSequenceId, lastSendSequenceNumber
                    });

                }
            } else {
                // track current so we can suppress duplicates later in the stream
                lastSendSequenceNumber.set(producerSequenceId);
            }
        }
        return canDispatch;
    }

    private long getStoredSequenceIdForMessage(MessageId messageId) {
        try {
            return brokerService.getPersistenceAdapter().getLastProducerSequenceId(messageId.getProducerId());
        } catch (IOException ignored) {
            LOG.debug("Failed to determine last producer sequence id for: {}", messageId, ignored);
        }
        return -1;
    }

    public void setLastStoredSequenceId(long l) {
        auditProducerSequenceIds = true;
        if (connectionContext.isNetworkConnection()) {
            brokerService = connectionContext.getBroker().getBrokerService();
            isNetworkProducer = true;
        }
        lastSendSequenceNumber.set(l);
        LOG.debug("last stored sequence id set: {}", l);
    }

    public void incrementSend() {
        flowControlInfo.incrementSend();
    }

    public void blockingOnFlowControl(boolean blockingOnFlowControl) {
        flowControlInfo.setBlockingOnFlowControl(blockingOnFlowControl);
    }

    public void incrementTimeBlocked(Destination destination, long timeBlocked) {
        flowControlInfo.incrementTimeBlocked(timeBlocked);
    }


    public boolean isBlockedForFlowControl() {
        return flowControlInfo.isBlockingOnFlowControl();
    }

    public void resetFlowControl() {
        flowControlInfo.reset();
    }

    public long getTotalTimeBlocked() {
        return flowControlInfo.getTotalTimeBlocked();
    }

    public int getPercentageBlocked() {
        double value = flowControlInfo.getSendsBlocked() / flowControlInfo.getTotalSends();
        return (int) value * 100;
    }


    public static class FlowControlInfo {
        private AtomicBoolean blockingOnFlowControl = new AtomicBoolean();
        private AtomicLong totalSends = new AtomicLong();
        private AtomicLong sendsBlocked = new AtomicLong();
        private AtomicLong totalTimeBlocked = new AtomicLong();


        public boolean isBlockingOnFlowControl() {
            return blockingOnFlowControl.get();
        }

        public void setBlockingOnFlowControl(boolean blockingOnFlowControl) {
            this.blockingOnFlowControl.set(blockingOnFlowControl);
            if (blockingOnFlowControl) {
                incrementSendBlocked();
            }
        }


        public long getTotalSends() {
            return totalSends.get();
        }

        public void incrementSend() {
            this.totalSends.incrementAndGet();
        }

        public long getSendsBlocked() {
            return sendsBlocked.get();
        }

        public void incrementSendBlocked() {
            this.sendsBlocked.incrementAndGet();
        }

        public long getTotalTimeBlocked() {
            return totalTimeBlocked.get();
        }

        public void incrementTimeBlocked(long time) {
            this.totalTimeBlocked.addAndGet(time);
        }

        public void reset() {
            blockingOnFlowControl.set(false);
            totalSends.set(0);
            sendsBlocked.set(0);
            totalTimeBlocked.set(0);

        }
    }
}
