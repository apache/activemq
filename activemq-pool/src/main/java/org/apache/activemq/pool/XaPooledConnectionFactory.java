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
package org.apache.activemq.pool;

import javax.transaction.TransactionManager;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * A pooled connection factory that automatically enlists
 * sessions in the current active XA transaction if any.
 */
public class XaPooledConnectionFactory extends PooledConnectionFactory {

    private TransactionManager transactionManager;
    
    public XaPooledConnectionFactory() {
        super();
    }

    public XaPooledConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    public XaPooledConnectionFactory(String brokerURL) {
        super(brokerURL);
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    protected ConnectionPool createConnectionPool(ActiveMQConnection connection) {
        return new XaConnectionPool(connection, getPoolFactory(), getTransactionManager());
    }

}
