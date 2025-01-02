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

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.jms.MessageFormatRuntimeException;

/**
 * Enforce message policies for JMS Header values
 *
 * @org.apache.xbean.XBean
 */
public class HeaderMessageInterceptorStrategy implements MessageInterceptorStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(HeaderMessageInterceptorStrategy.class);

    boolean forceDeliveryMode = false;

    boolean persistent = true;

    boolean forceExpiration = false;

    /**
    * variable which (when non-zero) is used to override
    * the expiration date for messages that arrive with
    * no expiration date set (in Milliseconds).
    */
    long zeroExpirationOverride = 0;

    /**
    * variable which (when non-zero) is used to limit
    * the expiration date (in Milliseconds).
    */
    long expirationCeiling = 0;

    /**
     * If true, the plugin will not update timestamp to past values
     * False by default
     */
    boolean futureOnly = false;

    /**
     * if true, update timestamp even if message has passed through a network
     * default false
     */
    boolean processNetworkMessages = false;

    /**
    * setter method for zeroExpirationOverride
    */
    public void setZeroExpirationOverride(long ttl)
    {
        this.zeroExpirationOverride = ttl;
    }

    /**
    * setter method for expirationCeiling
    */
    public void setExpirationCeiling(long expirationCeiling)
    {
        this.expirationCeiling = expirationCeiling;
    }

    public void setFutureOnly(boolean futureOnly) {
        this.futureOnly = futureOnly;
    }

    public void setProcessNetworkMessages(Boolean processNetworkMessages) {
        this.processNetworkMessages = processNetworkMessages;
    }

    @Override
    public void process(final ProducerBrokerExchange producerBrokerExchange, final Message message) throws MessageFormatRuntimeException {
        if(!isProcessNetworkMessages() && producerBrokerExchange.getConnectionContext().isNetworkConnection()) {
            // Message passed through a network and processNetworkMessages=true is not set
            return;
        }

        if(isForceExpiration()) {
            if (message.getTimestamp() > 0 && !message.getDestination().isDLQ()) {
                 long oldExpiration = message.getExpiration();
                 long newTimeStamp = System.currentTimeMillis();
                 long timeToLive = zeroExpirationOverride;
                 long oldTimestamp = message.getTimestamp();
                 if (oldExpiration > 0) {
                     timeToLive = oldExpiration - oldTimestamp;
                 }
                 if (timeToLive > 0 && expirationCeiling > 0 && timeToLive > expirationCeiling) {
                     timeToLive = expirationCeiling;
                 }
                 long expiration = timeToLive + newTimeStamp;
                 // In the scenario that the Broker is behind the clients we never want to set the
                 // Timestamp and Expiration in the past
                 if(!futureOnly || (expiration > oldExpiration)) {
                     if (timeToLive > 0 && expiration > 0) {
                         message.setExpiration(expiration);
                     }
                     message.setTimestamp(newTimeStamp);
                     LOG.debug("Set message {} timestamp from {} to {}", message.getMessageId(), oldTimestamp, newTimeStamp);
                 }
            }
        }

        if(forceDeliveryMode) {
            message.setPersistent(isPersistent());
        }
    }

    public void setForceDeliveryMode(boolean forceDeliveryMode) {
        this.forceDeliveryMode = forceDeliveryMode;
    }

    public boolean isForceDeliveryMode() {
        return this.forceDeliveryMode;
    }

    public void setForceExpiration(boolean forceExpiration) {
        this.forceExpiration = forceExpiration;
    }

    public boolean isForceExpiration() {
        return this.forceExpiration;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public boolean isPersistent() {
        return this.persistent;
    }

    public void setProcessNetworkMessages(boolean processNetworkMessages) {
        this.processNetworkMessages = processNetworkMessages;
    }

    public boolean isProcessNetworkMessages() {
        return this.processNetworkMessages;
    }
}
