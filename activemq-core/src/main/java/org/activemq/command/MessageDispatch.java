/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
package org.activemq.command;

import org.activemq.state.CommandVisitor;


/**
 * 
 * @openwire:marshaller
 * @version $Revision$
 */
public class MessageDispatch extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.MESSAGE_DISPATCH;

    protected ConsumerId consumerId;
    protected ActiveMQDestination destination;
    protected Message message;
    protected int redeliveryCounter;

    transient protected long deliverySequenceId;
    transient protected Object consumer;
    
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }
    
    public boolean isMessageDispatch() {
        return true;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ConsumerId getConsumerId() {
        return consumerId;
    }
    public void setConsumerId(ConsumerId consumerId) {
        this.consumerId = consumerId;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ActiveMQDestination getDestination() {
        return destination;
    }
    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }
    
    /**
     * @openwire:property version=1
     */
    public Message getMessage() {
        return message;
    }
    public void setMessage(Message message) {
        this.message = message;
    }

    public long getDeliverySequenceId() {
        return deliverySequenceId;
    }
    public void setDeliverySequenceId(long deliverySequenceId) {
        this.deliverySequenceId = deliverySequenceId;
    }
    
    /**
     * @openwire:property version=1
     */
    public int getRedeliveryCounter() {
        return redeliveryCounter;
    }
    public void setRedeliveryCounter(int deliveryCounter) {
        this.redeliveryCounter = deliveryCounter;
    }

    public Object getConsumer() {
        return consumer;
    }

    public void setConsumer(Object consumer) {
        this.consumer = consumer;
    }

    public Response visit(CommandVisitor visitor) throws Throwable {
        return null;
    }
    
}
