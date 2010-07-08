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

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.command.Message;
import org.apache.activemq.state.ProducerState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Holds internal state in the broker for a MessageProducer
 * 
 * @version $Revision: 1.8 $
 */
public class ProducerBrokerExchange {

    private static final Log LOG = LogFactory.getLog(ProducerBrokerExchange.class);
    private ConnectionContext connectionContext;
    private Destination regionDestination;
    private Region region;
    private ProducerState producerState;
    private boolean mutable = true;
    private long lastSendSequenceNumber = -1;
    
    public ProducerBrokerExchange() {
    }

    public ProducerBrokerExchange copy() {
        ProducerBrokerExchange rc = new ProducerBrokerExchange();
        rc.connectionContext = connectionContext.copy();
        rc.regionDestination = regionDestination;
        rc.region = region;
        rc.producerState = producerState;
        rc.mutable = mutable;
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
     * @param messageSend
     * @return false if message should be ignored as a duplicate
     */
    public boolean canDispatch(Message messageSend) {
        boolean canDispatch = true;
        if (lastSendSequenceNumber > 0) {
            if (messageSend.getMessageId().getProducerSequenceId() <= lastSendSequenceNumber) {
                canDispatch = false;
                LOG.debug("suppressing duplicate message send [" + messageSend.getMessageId() + "] with producerSequenceId [" 
                        + messageSend.getMessageId().getProducerSequenceId() + "] less than last stored: "  + lastSendSequenceNumber);
            }
        }
        return canDispatch;
    }

    public void setLastStoredSequenceId(long l) {
        lastSendSequenceNumber = l;
        LOG.debug("last stored sequence id set: " + l);
    }
}
