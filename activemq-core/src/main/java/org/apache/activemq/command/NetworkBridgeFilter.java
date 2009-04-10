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

import java.io.IOException;
import java.util.Arrays;

import javax.jms.JMSException;

import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @openwire:marshaller code="91"
 * @version $Revision: 1.12 $
 */
public class NetworkBridgeFilter implements DataStructure, BooleanExpression {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.NETWORK_BRIDGE_FILTER;
    static final Log LOG = LogFactory.getLog(NetworkBridgeFilter.class);

    private BrokerId networkBrokerId;
    private int networkTTL;

    public NetworkBridgeFilter() {
    }

    public NetworkBridgeFilter(BrokerId remoteBrokerPath, int networkTTL) {
        this.networkBrokerId = remoteBrokerPath;
        this.networkTTL = networkTTL;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public boolean matches(MessageEvaluationContext mec) throws JMSException {
        try {
            // for Queues - the message can be acknowledged and dropped whilst
            // still
            // in the dispatch loop
            // so need to get the reference to it
            Message message = mec.getMessage();
            return message != null && matchesForwardingFilter(message);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    public Object evaluate(MessageEvaluationContext message) throws JMSException {
        return matches(message) ? Boolean.TRUE : Boolean.FALSE;
    }

    protected boolean matchesForwardingFilter(Message message) {

        if (contains(message.getBrokerPath(), networkBrokerId)) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Message all ready routed once through this broker ("
                        + networkBrokerId + "), path: "
                        + Arrays.toString(message.getBrokerPath()) + " - ignoring: " + message);
            }
            return false;
        }

        int hops = message.getBrokerPath() == null ? 0 : message.getBrokerPath().length;

        if (hops >= networkTTL) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Message restricted to " + networkTTL + " network hops ignoring: " + message);
            }
            return false;
        }

        // Don't propagate advisory messages about network subscriptions
        if (message.isAdvisory() && message.getDataStructure() != null && message.getDataStructure().getDataStructureType() == CommandTypes.CONSUMER_INFO) {
            ConsumerInfo info = (ConsumerInfo)message.getDataStructure();
            hops = info.getBrokerPath() == null ? 0 : info.getBrokerPath().length;
            if (hops >= networkTTL) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("ConsumerInfo advisory restricted to " + networkTTL + " network hops ignoring: " + message);
                }
                return false;
            }
        }
        return true;
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

    /**
     * @openwire:property version=1
     */
    public int getNetworkTTL() {
        return networkTTL;
    }

    public void setNetworkTTL(int networkTTL) {
        this.networkTTL = networkTTL;
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

}
