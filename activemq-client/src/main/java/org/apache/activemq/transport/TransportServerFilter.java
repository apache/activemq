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
package org.apache.activemq.transport;

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.activemq.command.BrokerInfo;

public class TransportServerFilter implements TransportServer {

    protected final TransportServer next;

    /**
     * @param next
     */
    public TransportServerFilter(TransportServer next) {
        this.next = next;
    }

    @Override
    public URI getConnectURI() {
        return next.getConnectURI();
    }

    @Override
    public void setAcceptListener(TransportAcceptListener acceptListener) {
        next.setAcceptListener(acceptListener);
    }

    @Override
    public void setBrokerInfo(BrokerInfo brokerInfo) {
        next.setBrokerInfo(brokerInfo);
    }

    @Override
    public void start() throws Exception {
        next.start();
    }

    @Override
    public void stop() throws Exception {
        next.stop();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return next.getSocketAddress();
    }

    @Override
    public boolean isSslServer() {
        return next.isSslServer();
    }

    @Override
    public boolean isAllowLinkStealing() {
        return next.isAllowLinkStealing();
    }

    @Override
    public long getMaxConnectionExceededCount() {
        return next.getMaxConnectionExceededCount();
    }

    @Override
    public void resetStatistics() {
        next.resetStatistics();
    }
}
