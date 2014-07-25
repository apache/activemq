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
package org.apache.activemq.jms.pool;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.XASession;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.apache.geronimo.transaction.manager.WrapperNamedXAResource;

public class JcaConnectionPool extends XaConnectionPool {

    private final String name;

    public JcaConnectionPool(Connection connection, TransactionManager transactionManager, String name) {
        super(connection, transactionManager);
        this.name = name;
    }

    @Override
    protected XAResource createXaResource(PooledSession session) throws JMSException {
        XAResource xares = ((XASession)session.getInternalSession()).getXAResource();
        if (name != null) {
            xares = new WrapperNamedXAResource(xares, name);
        }
        return xares;
    }
}
