/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.activemq.ra;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import javax.resource.spi.ManagedConnection;
import javax.resource.ResourceException;

import org.apache.activemq.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @version $Rev$ $Date$
 */
public class JmsXAQueueTransactionTest extends JmsQueueTransactionTest {
    private static final String DEFAULT_HOST = "vm://localhost";

    private ConnectionManagerAdapter connectionManager = new ConnectionManagerAdapter();
    private ActiveMQManagedConnectionFactory managedConnectionFactory;
    private XAResource xaResource;
    private static long txGenerator;
    private Xid xid;

    @Override
    protected void setSessionTransacted() {
        resourceProvider.setTransacted(false);
        resourceProvider.setAckMode(Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    protected ConnectionFactory newConnectionFactory() throws Exception {
        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setServerUrl(DEFAULT_HOST);
        managedConnectionFactory.setUserName(org.apache.activemq.ActiveMQConnectionFactory.DEFAULT_USER);
        managedConnectionFactory.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);

        return (ConnectionFactory)managedConnectionFactory.createConnectionFactory(connectionManager);
    }


    /**
     * Recreates the connection.
     *
     * @throws javax.jms.JMSException
     */
    @Override
    protected void reconnect() throws Exception {
        super.reconnect();
        ManagedConnectionProxy proxy = (ManagedConnectionProxy) connection;
        ManagedConnection mc = proxy.getManagedConnection();
        xaResource = mc.getXAResource();
    }

    @Override
    protected ActiveMQPrefetchPolicy getPrefetchPolicy() {
        ManagedConnectionProxy proxy = (ManagedConnectionProxy) connection;
        ActiveMQManagedConnection mc = proxy.getManagedConnection();
        ActiveMQConnection conn = (ActiveMQConnection) mc.getPhysicalConnection();
        return conn.getPrefetchPolicy();
    }

    @Override
    protected void beginTx() throws Exception {
        xid = createXid();
        xaResource.start(xid, XAResource.TMNOFLAGS);
    }

    @Override
    protected void commitTx() throws Exception {
        xaResource.end(xid, XAResource.TMSUCCESS);
        int result = xaResource.prepare(xid);
        if (result == XAResource.XA_OK) {
            xaResource.commit(xid, false);
        }
        xid = null;
    }

    @Override
    protected void rollbackTx() throws Exception {
        xaResource.end(xid, XAResource.TMSUCCESS);
        xaResource.rollback(xid);
        xid = null;
    }

    //This test won't work with xa tx it is overridden to do nothing here
    @Override
    public void testMessageListener() throws Exception {
    }

    /**
     * Sends a batch of messages and validates that the message sent before
     * session close is not consumed.
     * <p/>
     * This test only works with local transactions, not xa. so its commented out here
     *
     * @throws Exception
     */
    @Override
    public void testSendSessionClose() throws Exception {
    }

    public Xid createXid() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            public int getFormatId() {
                return 86;
            }

            public byte[] getGlobalTransactionId() {
                return bs;
            }

            public byte[] getBranchQualifier() {
                return bs;
            }
        };

    }

}
