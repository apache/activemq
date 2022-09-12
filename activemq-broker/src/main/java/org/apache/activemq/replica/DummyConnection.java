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
package org.apache.activemq.replica;

import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.Connector;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.Response;

import java.io.IOException;

class DummyConnection implements Connection {
    @Override
    public Connector getConnector() {
        return null;
    }

    @Override
    public void dispatchSync(Command message) {
    }

    @Override
    public void dispatchAsync(Command command) {
    }

    @Override
    public Response service(Command command) {
        return null;
    }

    @Override
    public void serviceException(Throwable error) {
    }

    @Override
    public boolean isSlow() {
        return false;
    }

    @Override
    public boolean isBlocked() {
        return false;
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public int getDispatchQueueSize() {
        return 0;
    }

    @Override
    public ConnectionStatistics getStatistics() {
        return null;
    }

    @Override
    public boolean isManageable() {
        return false;
    }

    @Override
    public String getRemoteAddress() {
        return null;
    }

    @Override
    public void serviceExceptionAsync(IOException e) {

    }

    @Override
    public String getConnectionId() {
        return null;
    }

    @Override
    public boolean isNetworkConnection() {
        return false;
    }

    @Override
    public boolean isFaultTolerantConnection() {
        return false;
    }

    @Override
    public void updateClient(ConnectionControl control) {

    }

    @Override
    public int getActiveTransactionCount() {
        return 0;
    }

    @Override
    public Long getOldestActiveTransactionDuration() {
        return null;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }
}
