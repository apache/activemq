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
package org.apache.activemq.util;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.state.ProducerState;

/**
 * Utility class for re-sending messages
 *
 */
public final class BrokerSupport {

    private BrokerSupport() {        
    }
    
    /**
     * @param context
     * @param originalMessage 
     * @param deadLetterDestination
     * @throws Exception
     */
    public static void resend(final ConnectionContext context, Message originalMessage, ActiveMQDestination deadLetterDestination) throws Exception {
        Message message = originalMessage.copy();
        message.setOriginalDestination(message.getDestination());
        message.setOriginalTransactionId(message.getTransactionId());
        message.setDestination(deadLetterDestination);
        message.setTransactionId(null);
        boolean originalFlowControl = context.isProducerFlowControl();
        try {
            context.setProducerFlowControl(false);
            ProducerInfo info = new ProducerInfo();
            ProducerState state = new ProducerState(info);
            ProducerBrokerExchange producerExchange = new ProducerBrokerExchange();
            producerExchange.setProducerState(state);
            producerExchange.setMutable(true);
            producerExchange.setConnectionContext(context);
            context.getBroker().send(producerExchange, message);
        } finally {
            context.setProducerFlowControl(originalFlowControl);
        }
    }

}
