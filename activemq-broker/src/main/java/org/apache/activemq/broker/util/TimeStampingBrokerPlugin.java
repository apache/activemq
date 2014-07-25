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
package org.apache.activemq.broker.util;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Broker interceptor which updates a JMS Client's timestamp on the message
 * with a broker timestamp. Useful when the clocks on client machines are known
 * to not be correct and you can only trust the time set on the broker machines.
 *
 * Enabling this plugin will break JMS compliance since the timestamp that the
 * producer sees on the messages after as send() will be different from the
 * timestamp the consumer will observe when he receives the message. This plugin
 * is not enabled in the default ActiveMQ configuration.
 *
 * 2 new attributes have been added which will allow the administrator some override control
 * over the expiration time for incoming messages:
 *
 * Attribute 'zeroExpirationOverride' can be used to apply an expiration
 * time to incoming messages with no expiration defined (messages that would never expire)
 *
 * Attribute 'ttlCeiling' can be used to apply a limit to the expiration time
 *
 * @org.apache.xbean.XBean element="timeStampingBrokerPlugin"
 *
 *
 */
public class TimeStampingBrokerPlugin extends BrokerPluginSupport {
    private static final Logger LOG = LoggerFactory.getLogger(TimeStampingBrokerPlugin.class);
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
    long ttlCeiling = 0;

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
    * setter method for ttlCeiling
    */
    public void setTtlCeiling(long ttlCeiling)
    {
        this.ttlCeiling = ttlCeiling;
    }

    public void setFutureOnly(boolean futureOnly) {
        this.futureOnly = futureOnly;
    }

    public void setProcessNetworkMessages(Boolean processNetworkMessages) {
        this.processNetworkMessages = processNetworkMessages;
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {

        if (message.getTimestamp() > 0 && !isDestinationDLQ(message) &&
           (processNetworkMessages || (message.getBrokerPath() == null || message.getBrokerPath().length == 0))) {
            // timestamp not been disabled and has not passed through a network or processNetworkMessages=true

            long oldExpiration = message.getExpiration();
            long newTimeStamp = System.currentTimeMillis();
            long timeToLive = zeroExpirationOverride;
            long oldTimestamp = message.getTimestamp();
            if (oldExpiration > 0) {
                timeToLive = oldExpiration - oldTimestamp;
            }
            if (timeToLive > 0 && ttlCeiling > 0 && timeToLive > ttlCeiling) {
                timeToLive = ttlCeiling;
            }
            long expiration = timeToLive + newTimeStamp;
            // In the scenario that the Broker is behind the clients we never want to set the
            // Timestamp and Expiration in the past
            if(!futureOnly || (expiration > oldExpiration)) {
                if (timeToLive > 0 && expiration > 0) {
                    message.setExpiration(expiration);
                }
                message.setTimestamp(newTimeStamp);
                LOG.debug("Set message {} timestamp from {} to {}", new Object[]{ message.getMessageId(), oldTimestamp, newTimeStamp });
            }
        }
        super.send(producerExchange, message);
    }

    private boolean isDestinationDLQ(Message message) {
        DeadLetterStrategy deadLetterStrategy;
        Message tmp;

        Destination regionDestination = (Destination) message.getRegionDestination();
        if (message != null && regionDestination != null) {
            deadLetterStrategy = regionDestination.getDeadLetterStrategy();
            if (deadLetterStrategy != null && message.getOriginalDestination() != null) {
                // Cheap copy, since we only need two fields
                tmp = new ActiveMQMessage();
                tmp.setDestination(message.getOriginalDestination());
                tmp.setRegionDestination(regionDestination);

                // Determine if we are headed for a DLQ
                ActiveMQDestination deadLetterDestination = deadLetterStrategy.getDeadLetterQueueFor(tmp, null);
                if (deadLetterDestination.equals(message.getDestination())) {
                    return true;
                }
            }
        }
        return false;
    }
}
