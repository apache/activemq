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
package org.apache.activemq.network;

import java.util.Arrays;
import java.util.List;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.NetworkBridgeFilter;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * implement conditional behavior for queue consumers, allows replaying back to
 * origin if no consumers are present on the local broker after a configurable
 * delay, irrespective of the TTL. Also allows rate limiting of messages
 * through the network, useful for static includes
 *
 * @org.apache.xbean.XBean
 */
public class ConditionalNetworkBridgeFilterFactory implements NetworkBridgeFilterFactory {

    boolean replayWhenNoConsumers = false;
    int replayDelay = 0;
    int rateLimit = 0;
    int rateDuration = 1000;
    private boolean selectorAware = false;

    @Override
    public NetworkBridgeFilter create(ConsumerInfo info, BrokerId[] remoteBrokerPath, int messageTTL, int consumerTTL) {
        ConditionalNetworkBridgeFilter filter = new ConditionalNetworkBridgeFilter();
        filter.setNetworkBrokerId(remoteBrokerPath[0]);
        filter.setMessageTTL(messageTTL);
        filter.setConsumerTTL(consumerTTL);
        filter.setAllowReplayWhenNoConsumers(isReplayWhenNoConsumers());
        filter.setRateLimit(getRateLimit());
        filter.setRateDuration(getRateDuration());
        filter.setReplayDelay(getReplayDelay());
        filter.setSelectorAware(isSelectorAware());
        return filter;
    }

    public void setReplayWhenNoConsumers(boolean replayWhenNoConsumers) {
        this.replayWhenNoConsumers = replayWhenNoConsumers;
    }

    public boolean isReplayWhenNoConsumers() {
        return replayWhenNoConsumers;
    }

    public void setRateLimit(int rateLimit) {
        this.rateLimit = rateLimit;
    }

    public int getRateLimit() {
        return rateLimit;
    }

    public int getRateDuration() {
        return rateDuration;
    }

    public void setRateDuration(int rateDuration) {
        this.rateDuration = rateDuration;
    }

    public int getReplayDelay() {
        return replayDelay;
    }

    public void setReplayDelay(int replayDelay) {
        this.replayDelay = replayDelay;
    }

    public void setSelectorAware(boolean selectorAware) {
        this.selectorAware = selectorAware;
    }

    public boolean isSelectorAware() {
        return selectorAware;
    }

    private static class ConditionalNetworkBridgeFilter extends NetworkBridgeFilter {
        final static Logger LOG = LoggerFactory.getLogger(ConditionalNetworkBridgeFilter.class);
        private int rateLimit;
        private int rateDuration = 1000;
        private boolean allowReplayWhenNoConsumers = true;
        private int replayDelay = 1000;

        private int matchCount;
        private long rateDurationEnd;
        private boolean selectorAware = false;

        @Override
        protected boolean matchesForwardingFilter(Message message, final MessageEvaluationContext mec) {
            boolean match = true;
            if (mec.getDestination().isQueue() && contains(message.getBrokerPath(), networkBrokerId)) {
                // potential replay back to origin
                match = allowReplayWhenNoConsumers && hasNoLocalConsumers(message, mec) && hasNotJustArrived(message);

                if (match) {
                    LOG.trace("Replaying [{}] for [{}] back to origin in the absence of a local consumer", message.getMessageId(), message.getDestination());
                } else {
                    LOG.trace("Suppressing replay of [{}] for [{}] back to origin {}",
                            message.getMessageId(), message.getDestination(), Arrays.asList(message.getBrokerPath()));
                }

            } else {
                // use existing filter logic for topics and non replays
                match = super.matchesForwardingFilter(message, mec);
            }

            if (match && rateLimitExceeded()) {
                LOG.trace("Throttled network consumer rejecting [{}] for [{}] {}>{}/{}",
                        message.getMessageId(), message.getDestination(), matchCount, rateLimit, rateDuration);
                match = false;
            }

            return match;
        }

        private boolean hasNotJustArrived(Message message) {
            return replayDelay == 0 || (message.getBrokerInTime() + replayDelay < System.currentTimeMillis());
        }

        private boolean hasNoLocalConsumers(final Message message, final MessageEvaluationContext mec) {
            Destination regionDestination = (Destination) mec.getMessageReference().getRegionDestination();
            List<Subscription> consumers = regionDestination.getConsumers();
            for (Subscription sub : consumers) {
                if (!sub.getConsumerInfo().isNetworkSubscription() && !sub.getConsumerInfo().isBrowser()) {

                    if (!isSelectorAware()) {
                        LOG.trace("Not replaying [{}] for [{}] to origin due to existing local consumer: {}",
                                message.getMessageId(), message.getDestination(), sub.getConsumerInfo());
                        return false;

                    } else {
                        try {
                            if (sub.matches(message, mec)) {
                                LOG.trace("Not replaying [{}] for [{}] to origin due to existing selector matching local consumer: {}",
                                        message.getMessageId(), message.getDestination(), sub.getConsumerInfo());
                                return false;
                            }
                        } catch (Exception ignored) {}
                    }
                }
            }
            return true;
        }

        private boolean rateLimitExceeded() {
            if (rateLimit == 0) {
                return false;
            }

            if (rateDurationEnd < System.currentTimeMillis()) {
                rateDurationEnd = System.currentTimeMillis() + rateDuration;
                matchCount = 0;
            }
            return ++matchCount > rateLimit;
        }

        public void setReplayDelay(int replayDelay) {
            this.replayDelay = replayDelay;
        }

        public void setRateLimit(int rateLimit) {
            this.rateLimit = rateLimit;
        }

        public void setRateDuration(int rateDuration) {
            this.rateDuration = rateDuration;
        }

        public void setAllowReplayWhenNoConsumers(boolean allowReplayWhenNoConsumers) {
            this.allowReplayWhenNoConsumers = allowReplayWhenNoConsumers;
        }

        public void setSelectorAware(boolean selectorAware) {
            this.selectorAware = selectorAware;
        }

        public boolean isSelectorAware() {
            return selectorAware;
        }
    }
}
