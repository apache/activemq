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

import javax.jms.JMSException;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdvisoryConsumer implements ActiveMQDispatcher {
    private static final transient Logger LOG = LoggerFactory.getLogger(AdvisoryConsumer.class);

    int deliveredCounter;

    private final ActiveMQConnection connection;
    private ConsumerInfo info;
    private boolean closed;

    public AdvisoryConsumer(ActiveMQConnection connection, ConsumerId consumerId) throws JMSException {
        this.connection = connection;
        info = new ConsumerInfo(consumerId);
        info.setDestination(AdvisorySupport.TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC);
        info.setPrefetchSize(1000);
        info.setNoLocal(true);
        info.setDispatchAsync(true);

        this.connection.addDispatcher(info.getConsumerId(), this);
        this.connection.syncSendPacket(this.info);
    }

    public synchronized void dispose() {
        if (!closed) {
            try {
                this.connection.asyncSendPacket(info.createRemoveCommand());
            } catch (JMSException e) {
                LOG.debug("Failed to send remove command: " + e, e);
            }
            this.connection.removeDispatcher(info.getConsumerId());
            closed = true;
        }
    }

    public void dispatch(MessageDispatch md) {

        // Auto ack messages when we reach 75% of the prefetch
        deliveredCounter++;
        if (deliveredCounter > (0.75 * info.getPrefetchSize())) {
            try {
                MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, deliveredCounter);
                connection.asyncSendPacket(ack);
                deliveredCounter = 0;
            } catch (JMSException e) {
                connection.onClientInternalException(e);
            }
        }

        DataStructure o = md.getMessage().getDataStructure();
        if (o != null && o.getClass() == DestinationInfo.class) {
            processDestinationInfo((DestinationInfo)o);
        } else {
            //This can happen across networks
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected message was dispatched to the AdvisoryConsumer: "+md);
            }
        }

    }

    private void processDestinationInfo(DestinationInfo dinfo) {
        ActiveMQDestination dest = dinfo.getDestination();
        if (!dest.isTemporary()) {
            return;
        }

        ActiveMQTempDestination tempDest = (ActiveMQTempDestination)dest;
        if (dinfo.getOperationType() == DestinationInfo.ADD_OPERATION_TYPE) {
            if (tempDest.getConnection() != null) {
                tempDest = (ActiveMQTempDestination) tempDest.createDestination(tempDest.getPhysicalName());
            }
            connection.activeTempDestinations.put(tempDest, tempDest);
        } else if (dinfo.getOperationType() == DestinationInfo.REMOVE_OPERATION_TYPE) {
            connection.activeTempDestinations.remove(tempDest);
        }
    }

}
