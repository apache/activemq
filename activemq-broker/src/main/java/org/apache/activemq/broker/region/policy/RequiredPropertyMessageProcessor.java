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
package org.apache.activemq.broker.region.policy;

import java.io.IOException;

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.MessageFormatRuntimeException;

/**
 * Require message properties 
 *
 * @org.apache.xbean.XBean
 */
public class RequiredPropertyMessageProcessor implements MessageProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RequiredPropertyMessageProcessor.class);

    private boolean enabled = true;

    private String[] requiredProperties;

    public RequiredPropertyMessageProcessor() {
        LOG.info("RequiredPropertyMessageInterceptor initialized");
    }

    @Override
    public void process(ProducerBrokerExchange producerExchange, Message message) throws MessageFormatRuntimeException {
        if(isEnabled()) {
            for(String requiredProperty : requiredProperties) {
                try {
                    if(message.getProperty(requiredProperty) == null) {
                        LOG.warn("Message id:{} for dest:{} does not contain required property:{}", message.getMessageId(), message.getDestination(), requiredProperty);
                        throw new MessageFormatRuntimeException("Policy requires message property:" + requiredProperty, "");
                    }
                } catch (IOException e) {
                    throw new MessageFormatRuntimeException("Error checking for required property:" + requiredProperty, "", e);
                }
            }
        }
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return this.enabled;
    }

    public void setRequiredProperties(String[] requiredProperties) {
        this.requiredProperties = requiredProperties;
    }

    public String[] getRequiredProperties() {
        return this.requiredProperties;
    }
}
