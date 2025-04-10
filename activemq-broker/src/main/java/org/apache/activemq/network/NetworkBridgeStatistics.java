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

import java.util.Set;

import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.StatsImpl;

/**
 * The Statistics for a NetworkBridge.
 */
public class NetworkBridgeStatistics extends StatsImpl {

    protected CountStatisticImpl enqueues;
    protected CountStatisticImpl dequeues;
    protected CountStatisticImpl receivedCount;
    protected CountStatisticImpl localExceptionCount;
    protected CountStatisticImpl remoteExceptionCount;

    public NetworkBridgeStatistics() {
        enqueues = new CountStatisticImpl("enqueues", "The current number of enqueues this bridge has, which is the number of potential messages to be forwarded.");
        dequeues = new CountStatisticImpl("dequeues", "The current number of dequeues this bridge has, which is the number of messages received by the remote broker.");
        receivedCount = new CountStatisticImpl("receivedCount", "The number of messages that have been received by the NetworkBridge from the remote broker.  Only applies for Duplex bridges.");
        localExceptionCount = new CountStatisticImpl("localExceptionCount", "The number of exceptions that have been received by the NetworkBridge from the local broker.");
        remoteExceptionCount = new CountStatisticImpl("remoteExceptionCount", "The number of exceptions that have been received by the NetworkBridge from the remote broker.");

        addStatistics(Set.of(enqueues, dequeues, receivedCount, localExceptionCount, remoteExceptionCount));
    }

    /**
     * The current number of enqueues this bridge has, which is the number of potential messages to be forwarded
     * Messages may not be forwarded if there is no subscription
     *
     * @return
     */
    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    /**
     * The current number of dequeues this bridge has, which is the number of
     * messages actually sent to and received by the remote broker.
     *
     * @return
     */
    public CountStatisticImpl getDequeues() {
        return dequeues;
    }

    /**
     * The number of messages that have been received by the NetworkBridge from the remote broker.
     * Only applies for Duplex bridges.
     *
     * @return
     */
    public CountStatisticImpl getReceivedCount() {
        return receivedCount;
    }

    /**
     * The current number of exceptions this bridge has, which is the number of
     * exceptions received from the remote broker.
     *
     * @return
     */
    public CountStatisticImpl getLocalExceptionCount() {
        return localExceptionCount;
    }

    /**
     * The current number of exceptions this bridge has, which is the number of
     * exceptions received from the remote broker.
     *
     * @return
     */
    public CountStatisticImpl getRemoteExceptionCount() {
        return remoteExceptionCount;
    }

    @Override
    public void reset() {
        if (this.isDoReset()) {
            super.reset();
            enqueues.reset();
            dequeues.reset();
            receivedCount.reset();
            localExceptionCount.reset();
            remoteExceptionCount.reset();
        }
    }

    @Override
    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dequeues.setEnabled(enabled);
        receivedCount.setEnabled(enabled);
        localExceptionCount.setEnabled(enabled);
        remoteExceptionCount.setEnabled(enabled);
    }

    public void setParent(NetworkBridgeStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.enqueues);
            dequeues.setParent(parent.dequeues);
            receivedCount.setParent(parent.receivedCount);
            localExceptionCount.setParent(parent.localExceptionCount);
            remoteExceptionCount.setParent(parent.remoteExceptionCount);
        } else {
            enqueues.setParent(null);
            dequeues.setParent(null);
            receivedCount.setParent(null);
            localExceptionCount.setParent(null);
            remoteExceptionCount.setParent(null);
        }
    }

}
