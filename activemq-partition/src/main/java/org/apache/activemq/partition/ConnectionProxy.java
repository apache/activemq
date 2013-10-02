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
package org.apache.activemq.partition;

import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.Response;

import java.io.IOException;

/**
 * A Connection implementation that proxies all Connection invocation to
 * a delegate connection.
 */
public class ConnectionProxy implements Connection {
    final Connection next;

    public ConnectionProxy(Connection next) {
        this.next = next;
    }

    @Override
    public void dispatchAsync(Command command) {
        next.dispatchAsync(command);
    }

    @Override
    public void dispatchSync(Command message) {
        next.dispatchSync(message);
    }

    @Override
    public String getConnectionId() {
        return next.getConnectionId();
    }

    @Override
    public Connector getConnector() {
        return next.getConnector();
    }

    @Override
    public int getDispatchQueueSize() {
        return next.getDispatchQueueSize();
    }

    @Override
    public String getRemoteAddress() {
        return next.getRemoteAddress();
    }

    @Override
    public ConnectionStatistics getStatistics() {
        return next.getStatistics();
    }

    @Override
    public boolean isActive() {
        return next.isActive();
    }

    @Override
    public boolean isBlocked() {
        return next.isBlocked();
    }

    @Override
    public boolean isConnected() {
        return next.isConnected();
    }

    @Override
    public boolean isFaultTolerantConnection() {
        return next.isFaultTolerantConnection();
    }

    @Override
    public boolean isManageable() {
        return next.isManageable();
    }

    @Override
    public boolean isNetworkConnection() {
        return next.isNetworkConnection();
    }

    @Override
    public boolean isSlow() {
        return next.isSlow();
    }

    @Override
    public Response service(Command command) {
        return next.service(command);
    }

    @Override
    public void serviceException(Throwable error) {
        next.serviceException(error);
    }

    @Override
    public void serviceExceptionAsync(IOException e) {
        next.serviceExceptionAsync(e);
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
    public void updateClient(ConnectionControl control) {
        next.updateClient(control);
    }
}
