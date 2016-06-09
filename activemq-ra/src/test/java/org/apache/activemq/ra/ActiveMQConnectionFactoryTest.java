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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import javax.transaction.xa.XAResource;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQTopicSubscriber;
import org.junit.Before;
import org.junit.Test;

public class ActiveMQConnectionFactoryTest {

    private ActiveMQManagedConnectionFactory mcf;
    private ActiveMQConnectionRequestInfo info;
    private String url = "vm://localhost?broker.persistent=false";
    private String user = "defaultUser";
    private String pwd = "defaultPasswd";

    @Before
    public void setUp() throws Exception {
        mcf = new ActiveMQManagedConnectionFactory();
        info = new ActiveMQConnectionRequestInfo();
        info.setServerUrl(url);
        info.setUserName(user);
        info.setPassword(pwd);
        info.setAllPrefetchValues(new Integer(100));
    }

    @Test(timeout = 60000)
    public void testSerializability() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(mcf, new ConnectionManagerAdapter(), info);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(factory);
        oos.close();
        byte[] byteArray = bos.toByteArray();

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        ActiveMQConnectionFactory deserializedFactory = (ActiveMQConnectionFactory) ois.readObject();
        ois.close();

        Connection con = deserializedFactory.createConnection("defaultUser", "defaultPassword");
        ActiveMQConnection connection = ((ActiveMQConnection) ((ManagedConnectionProxy) con).getManagedConnection().getPhysicalConnection());
        assertEquals(100, connection.getPrefetchPolicy().getQueuePrefetch());
        assertNotNull("Connection object returned by ActiveMQConnectionFactory.createConnection() is null", con);

        connection.close();
    }

    @Test(timeout = 60000)
    public void testOptimizeDurablePrefetch() throws Exception {
        ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
        ra.setServerUrl(url);
        ra.setUserName(user);
        ra.setPassword(pwd);

        ra.setOptimizeDurableTopicPrefetch(0);
        ra.setDurableTopicPrefetch(0);

        Connection con = ra.makeConnection();

        con.setClientID("x");
        Session sess = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber sub = sess.createDurableSubscriber(sess.createTopic("TEST"), "x");
        con.start();

        assertEquals(0, ((ActiveMQTopicSubscriber) sub).getPrefetchNumber());

        con.close();
    }

    @Test(timeout = 60000)
    public void testGetXAResource() throws Exception {
        ActiveMQResourceAdapter ra = new ActiveMQResourceAdapter();
        ra.setServerUrl(url);
        ra.setUserName(user);
        ra.setPassword(pwd);

        XAResource[] resources = ra.getXAResources(null);
        assertEquals("one resource", 1, resources.length);

        assertEquals("no pending transactions", 0, resources[0].recover(100).length);

        // validate equality
        XAResource[] resource2 = ra.getXAResources(null);
        assertEquals("one resource", 1, resource2.length);
        assertTrue("isSameRM true", resources[0].isSameRM(resource2[0]));
        assertFalse("no tthe same instance", resources[0].equals(resource2[0]));
    }
}
