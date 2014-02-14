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
package org.apache.activemq.broker.jmx;

import java.io.IOException;
import java.util.Set;

import javax.management.ObjectName;

import org.apache.activemq.broker.Connection;
import org.apache.activemq.util.IOExceptionSupport;

public class ConnectionView implements ConnectionViewMBean {

    private final Connection connection;
    private final ManagementContext managementContext;
    private String userName;

    public ConnectionView(Connection connection) {
        this(connection, null);
    }

    public ConnectionView(Connection connection, ManagementContext managementContext) {
        this.connection = connection;
        this.managementContext = managementContext;
    }

    @Override
    public void start() throws Exception {
        connection.start();
    }

    @Override
    public void stop() throws Exception {
        connection.stop();
    }

    /**
     * @return true if the Connection is slow
     */
    @Override
    public boolean isSlow() {
        return connection.isSlow();
    }

    /**
     * @return if after being marked, the Connection is still writing
     */
    @Override
    public boolean isBlocked() {
        return connection.isBlocked();
    }

    /**
     * @return true if the Connection is connected
     */
    @Override
    public boolean isConnected() {
        return connection.isConnected();
    }

    /**
     * @return true if the Connection is active
     */
    @Override
    public boolean isActive() {
        return connection.isActive();
    }

    @Override
    public int getDispatchQueueSize() {
        return connection.getDispatchQueueSize();
    }

    /**
     * Resets the statistics
     */
    @Override
    public void resetStatistics() {
        connection.getStatistics().reset();
    }

    @Override
    public String getRemoteAddress() {
        return connection.getRemoteAddress();
    }

    @Override
    public String getClientId() {
        return connection.getConnectionId();
    }

    public String getConnectionId() {
        return connection.getConnectionId();
    }

    @Override
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public ObjectName[] getConsumers() {
        ObjectName[] result = null;

        if (connection != null && managementContext != null) {

            try {
                ObjectName query = createConsumerQueury(connection.getConnectionId());
                Set<ObjectName> names = managementContext.queryNames(query, null);
                result = names.toArray(new ObjectName[0]);
            } catch (Exception e) {
            }
        }

        return result;
    }

    @Override
    public ObjectName[] getProducers() {
        ObjectName[] result = null;

        if (connection != null && managementContext != null) {

            try {
                ObjectName query = createProducerQueury(connection.getConnectionId());
                Set<ObjectName> names = managementContext.queryNames(query, null);
                result = names.toArray(new ObjectName[0]);
            } catch (Exception e) {
            }
        }

        return result;
    }

    private ObjectName createConsumerQueury(String clientId) throws IOException {
        try {
            return BrokerMBeanSupport.createConsumerQueury(managementContext.getJmxDomainName(), clientId);
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

    private ObjectName createProducerQueury(String clientId) throws IOException {
        try {
            return BrokerMBeanSupport.createProducerQueury(managementContext.getJmxDomainName(), clientId);
        } catch (Throwable e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public int getActiveTransactionCount() {
        return connection.getActiveTransactionCount();
    }

    @Override
    public Long getOldestActiveTransactionDuration() {
        return connection.getOldestActiveTransactionDuration();
    }
}
