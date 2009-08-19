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
import javax.jms.Session;
import javax.jms.Connection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import javax.resource.spi.ManagedConnection;
import javax.resource.ResourceException;

import org.apache.activemq.*;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @version $Rev$ $Date$
 */
public class JmsXARollback2CxTransactionTest extends JmsQueueTransactionTest {
    private static final String DEFAULT_HOST = "vm://localhost";

    private ConnectionManagerAdapter connectionManager = new ConnectionManagerAdapter();
    private static long txGenerator;
    private Xid xid;
    private XAResource[] xares = new XAResource[2];
    private int index = 0;

    @Override
    protected void setSessionTransacted() {
        resourceProvider.setTransacted(false);
        resourceProvider.setAckMode(Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    protected ConnectionFactory newConnectionFactory() throws Exception {
        ActiveMQManagedConnectionFactory managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setServerUrl(DEFAULT_HOST);
        managedConnectionFactory.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        managedConnectionFactory.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);

        return (ConnectionFactory) managedConnectionFactory.createConnectionFactory(connectionManager);
    }

    public void xtestRepeatReceiveTwoThenRollback() throws Exception {
        for (index = 0; index< 20; index ++) {
            testReceiveTwoThenRollback();
        }
    }

    /**
     * Recreates the connection.
     *
     * @throws javax.jms.JMSException
     */
    @Override
    protected void reconnect() throws Exception {
        super.reconnect();
        xares[0] = getXAResource(connection);
        ManagedConnectionProxy cx2 = (ManagedConnectionProxy) connectionFactory.createConnection();
        xares[1] = getXAResource(cx2);
    }

    private XAResource getXAResource(Connection connection) throws ResourceException {
        ManagedConnectionProxy proxy = (ManagedConnectionProxy) connection;
        ManagedConnection mc = proxy.getManagedConnection();
        return mc.getXAResource();
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
        xares[index%2].start(xid, XAResource.TMNOFLAGS);
        xares[(index+ 1)%2].start(xid, XAResource.TMJOIN);
    }

    @Override
    protected void commitTx() throws Exception {
        xares[index%2].end(xid, XAResource.TMSUCCESS);
        xares[(index+ 1)%2].end(xid, XAResource.TMSUCCESS);
        int result = xares[index%2].prepare(xid);
        if (result == XAResource.XA_OK) {
            xares[index%2].commit(xid, false);
        }
        xid = null;
    }

    @Override
    protected void rollbackTx() throws Exception {
        xares[index%2].end(xid, XAResource.TMSUCCESS);
        xares[(index+ 1)%2].end(xid, XAResource.TMSUCCESS);
        xares[index%2].rollback(xid);
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