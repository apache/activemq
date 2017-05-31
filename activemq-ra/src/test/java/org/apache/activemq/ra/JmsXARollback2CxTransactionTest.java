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
package org.apache.activemq.ra;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.JmsQueueTransactionTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsXARollback2CxTransactionTest extends JmsQueueTransactionTest {

    protected static final Logger LOG = LoggerFactory.getLogger(JmsXARollback2CxTransactionTest.class);

    private static final String DEFAULT_HOST = "vm://localhost?create=false";

    private ManagedConnectionProxy cx2;
    private ConnectionManagerAdapter connectionManager = new ConnectionManagerAdapter();
    private static long txGenerator;
    private Xid xid;
    private XAResource[] xares = new XAResource[2];
    private int index = 0;

    @Override
    protected void setUp() throws Exception {
        LOG.info("Starting ----------------------------> {}", this.getName());
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        if (cx2 != null) {
            cx2.close();
        }

        super.tearDown();
    }

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

    public void testReconnectWithClientId() throws Exception {
        for (index = 0; index< 20; index ++) {
            reconnect();
        }
    }

    public void testRepeatReceiveTwoThenRollback() throws Exception {
        for (index = 0; index< 2; index ++) {
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
        cx2 = (ManagedConnectionProxy) connectionFactory.createConnection();
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
            @Override
            public int getFormatId() {
                return 86;
            }

            @Override
            public byte[] getGlobalTransactionId() {
                return bs;
            }

            @Override
            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }
}