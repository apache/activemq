/*
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
package org.apache.activemq.transport.amqp.message;

import static org.apache.activemq.transport.amqp.message.AmqpMessageSupport.JMS_AMQP_NATIVE;

import javax.jms.BytesMessage;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMessage;

public class AutoOutboundTransformer extends JMSMappingOutboundTransformer {

    private final JMSMappingOutboundTransformer transformer = new JMSMappingOutboundTransformer();

    @Override
    public EncodedMessage transform(ActiveMQMessage message) throws Exception {
        if (message == null) {
            return null;
        }

        if (message.getBooleanProperty(JMS_AMQP_NATIVE)) {
            if (message instanceof BytesMessage) {
                return AMQPNativeOutboundTransformer.transform(this, (ActiveMQBytesMessage) message);
            } else {
                return null;
            }
        } else {
            return transformer.transform(message);
        }
    }
}
