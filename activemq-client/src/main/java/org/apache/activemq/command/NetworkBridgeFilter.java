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
package org.apache.activemq.command;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.Arrays;

/**
 * @openwire:marshaller code="91"
 *
 */
public class NetworkBridgeFilter implements DataStructure, BooleanExpression {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.NETWORK_BRIDGE_FILTER;
    static final Logger LOG = LoggerFactory.getLogger(NetworkBridgeFilter.class);

    protected BrokerId networkBrokerId;
    protected int messageTTL;
    protected int consumerTTL;
    transient ConsumerInfo consumerInfo;

    public NetworkBridgeFilter() {
    }

    public NetworkBridgeFilter(ConsumerInfo consumerInfo, BrokerId networkBrokerId, int messageTTL, int consumerTTL) {
        this.networkBrokerId = networkBrokerId;
        this.messageTTL = messageTTL;
        this.consumerTTL = consumerTTL;
        this.consumerInfo = consumerInfo;
    }

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public boolean isMarshallAware() {
        return false;
    }

    @Override
    public boolean matches(MessageEvaluationContext mec) throws JMSException {
        try {
            // for Queues - the message can be acknowledged and dropped whilst
            // still
            // in the dispatch loop
            // so need to get the reference to it
            Message message = mec.getMessage();
            return message != null && matchesForwardingFilter(message, mec);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    public Object evaluate(MessageEvaluationContext message) throws JMSException {
        return matches(message) ? Boolean.TRUE : Boolean.FALSE;
    }

    protected boolean matchesForwardingFilter(Message message, MessageEvaluationContext mec) {

        if (contains(message.getBrokerPath(), networkBrokerId)) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Message all ready routed once through target broker ("
                        + networkBrokerId + "), path: "
                        + Arrays.toString(message.getBrokerPath()) + " - ignoring: " + message);
            }
            return false;
        }

        int hops = message.getBrokerPath() == null ? 0 : message.getBrokerPath().length;

        if (messageTTL > -1 && hops >= messageTTL) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Message restricted to " + messageTTL + " network hops ignoring: " + message);
            }
            return false;
        }

        if (message.isAdvisory()) {
            if (consumerInfo != null && consumerInfo.isNetworkSubscription() && isAdvisoryInterpretedByNetworkBridge(message)) {
                // they will be interpreted by the bridge leading to dup commands
                if (LOG.isTraceEnabled()) {
                    LOG.trace("not propagating advisory to network sub: " + consumerInfo.getConsumerId() + ", message: "+ message);
                }
                return false;
            } else if ( message.getDataStructure() != null && message.getDataStructure().getDataStructureType() == CommandTypes.CONSUMER_INFO) {
                ConsumerInfo info = (ConsumerInfo)message.getDataStructure();
                hops = info.getBrokerPath() == null ? 0 : info.getBrokerPath().length;
                if (consumerTTL > -1 && hops >= consumerTTL) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("ConsumerInfo advisory restricted to " + consumerTTL + " network hops ignoring: " + message);
                    }
                    return false;
                }

                if (contains(info.getBrokerPath(), networkBrokerId)) {
                    LOG.trace("ConsumerInfo advisory all ready routed once through target broker ("
                            + networkBrokerId + "), path: "
                            + Arrays.toString(info.getBrokerPath()) + " - ignoring: " + message);
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isAdvisoryInterpretedByNetworkBridge(Message message) {
        return AdvisorySupport.isConsumerAdvisoryTopic(message.getDestination()) ||
                AdvisorySupport.isVirtualDestinationConsumerAdvisoryTopic(message.getDestination()) ||
                AdvisorySupport.isTempDestinationAdvisoryTopic(message.getDestination());
    }

    public static boolean contains(BrokerId[] brokerPath, BrokerId brokerId) {
        if (brokerPath != null && brokerId != null) {
            for (int i = 0; i < brokerPath.length; i++) {
                if (brokerId.equals(brokerPath[i])) {
                    return true;
                }
            }
        }
        return false;
    }

    // keep for backward compat with older
    // wire formats
    public int getNetworkTTL() {
        return messageTTL;
    }

    public void setNetworkTTL(int networkTTL) {
        messageTTL = networkTTL;
        consumerTTL = networkTTL;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public BrokerId getNetworkBrokerId() {
        return networkBrokerId;
    }

    public void setNetworkBrokerId(BrokerId remoteBrokerPath) {
        this.networkBrokerId = remoteBrokerPath;
    }

    public void setMessageTTL(int messageTTL) {
        this.messageTTL = messageTTL;
    }

    /**
     * @openwire:property version=10
     */
    public int getMessageTTL() {
        return this.messageTTL;
    }

    public void setConsumerTTL(int consumerTTL) {
        this.consumerTTL = consumerTTL;
    }

    /**
     * @openwire:property version=10
     */
    public int getConsumerTTL() {
        return this.consumerTTL;
    }
}
