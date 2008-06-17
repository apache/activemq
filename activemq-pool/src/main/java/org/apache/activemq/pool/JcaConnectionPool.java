/*
 * Copyright 2006 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import javax.jms.JMSException;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ra.LocalAndXATransaction;
import org.apache.commons.pool.ObjectPoolFactory;
import org.apache.geronimo.transaction.manager.WrapperNamedXAResource;

public class JcaConnectionPool extends XaConnectionPool {

    private String name;

    public JcaConnectionPool(ActiveMQConnection connection, ObjectPoolFactory poolFactory, TransactionManager transactionManager, String name) {
        super(connection, poolFactory, transactionManager);
        this.name = name;
    }

    protected XAResource createXaResource(PooledSession session) throws JMSException {
        XAResource xares = new LocalAndXATransaction(session.getSession().getTransactionContext());
        if (name != null) {
            xares = new WrapperNamedXAResource(xares, name);
        }
        return xares;
    }

}
