/*
 *  Copyright 2008 hak8fe.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package org.apache.activemq.ra;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Timer;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.TopicSubscriber;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQTopicSubscriber;

/**
 *
 * @author hak8fe
 */
public class ActiveMQConnectionFactoryTest extends TestCase {
    
    ActiveMQManagedConnectionFactory mcf;
    ActiveMQConnectionRequestInfo info;
    String url = "vm://localhost";
    String user = "defaultUser";
    String pwd = "defaultPasswd";
    
    public ActiveMQConnectionFactoryTest(String testName) {
        super(testName);
    }            

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mcf = new ActiveMQManagedConnectionFactory();
        info = new ActiveMQConnectionRequestInfo();
        info.setServerUrl(url);
        info.setUserName(user);
        info.setPassword(pwd);
        info.setAllPrefetchValues(new Integer(100));
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSerializability() throws Exception
    {
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
        ActiveMQConnection connection = ((ActiveMQConnection)((ManagedConnectionProxy)con).getManagedConnection().getPhysicalConnection());
        assertEquals(100, connection.getPrefetchPolicy().getQueuePrefetch());
        assertNotNull("Connection object returned by ActiveMQConnectionFactory.createConnection() is null", con);
    }

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

        assertEquals(0, ((ActiveMQTopicSubscriber)sub).getPrefetchNumber());
    }

}
