/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Timer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.resource.Referenceable;
import javax.resource.ResourceException;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.work.WorkManager;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ra.ActiveMQConnectionRequestInfo;
import org.apache.activemq.ra.ActiveMQManagedConnectionFactory;
import org.apache.activemq.ra.ActiveMQResourceAdapter;
import org.apache.activemq.ra.ManagedConnectionProxy;


/**
 * @version $Revision$
 */
public class ManagedConnectionFactoryTest extends TestCase {
    
    private static final String DEFAULT_HOST = "vm://localhost?broker.persistent=false";
    private static final String REMOTE_HOST = "vm://remotehost?broker.persistent=false";
    private ActiveMQManagedConnectionFactory managedConnectionFactory;
    
    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        
    	ActiveMQResourceAdapter adapter = new ActiveMQResourceAdapter(); 
    	adapter.setServerUrl(DEFAULT_HOST);
    	adapter.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
    	adapter.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);
    	adapter.start(new BootstrapContext(){
			public WorkManager getWorkManager() {
				return null;
			}
			public XATerminator getXATerminator() {
				return null;
			}

			public Timer createTimer() throws UnavailableException {
				return null;
			}
		});
    	
        managedConnectionFactory = new ActiveMQManagedConnectionFactory();
        managedConnectionFactory.setResourceAdapter(adapter);
        
    }
    
    public void testConnectionFactoryAllocation() throws ResourceException, JMSException {
        
        // Make sure that the ConnectionFactory is asking the connection manager to
        // allocate the connection.
        final boolean allocateRequested[] = new boolean[]{false};
        Object cf = managedConnectionFactory.createConnectionFactory(
            new ConnectionManagerAdapter() {
                private static final long serialVersionUID = 1699499816530099939L;

                public Object allocateConnection(ManagedConnectionFactory connectionFactory, ConnectionRequestInfo info)
                        throws ResourceException {
                    allocateRequested[0]=true;
                    return super.allocateConnection(connectionFactory, info);
                }
            }    
        );
        
        // We should be getting a JMS Connection Factory.
        assertTrue( cf instanceof ConnectionFactory );
        ConnectionFactory connectionFactory = (ConnectionFactory)cf;
        
        // Make sure that the connection factory is using the ConnectionManager..
        Connection connection = connectionFactory.createConnection();        
        assertTrue(allocateRequested[0]);
        
        // Make sure that the returned connection is of the expected type.
        assertTrue( connection!=null );
        assertTrue( connection instanceof ManagedConnectionProxy );
        
    }

    
    public void testConnectionFactoryConnectionMatching() throws ResourceException, JMSException {
        
        ActiveMQConnectionRequestInfo ri1 = new ActiveMQConnectionRequestInfo();
        ri1.setServerUrl(DEFAULT_HOST);
        ri1.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        ri1.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);

        ActiveMQConnectionRequestInfo ri2 = new ActiveMQConnectionRequestInfo();
        ri2.setServerUrl(REMOTE_HOST);
        ri2.setUserName(ActiveMQConnectionFactory.DEFAULT_USER);
        ri2.setPassword(ActiveMQConnectionFactory.DEFAULT_PASSWORD);
        assertNotSame(ri1, ri2);
        
        ManagedConnection connection1 = managedConnectionFactory.createManagedConnection(null, ri1);
        ManagedConnection connection2 = managedConnectionFactory.createManagedConnection(null, ri2);        
        assertTrue(connection1!=connection2);
        
        HashSet set = new HashSet();
        set.add(connection1);
        set.add(connection2);
        
        // Can we match for the first connection?
        ActiveMQConnectionRequestInfo ri3 = ri1.copy();
        assertTrue( ri1!=ri3 && ri1.equals(ri3) );
        ManagedConnection test = managedConnectionFactory.matchManagedConnections(set,null, ri3);
        assertTrue( connection1==test );

        // Can we match for the second connection?
        ri3 = ri2.copy();
        assertTrue( ri2!=ri3 && ri2.equals(ri3) );
        test = managedConnectionFactory.matchManagedConnections(set,null, ri2);
        assertTrue( connection2==test );
        
    }
    
    public void testConnectionFactoryIsSerializableAndReferenceable() throws ResourceException, JMSException {
        Object cf = managedConnectionFactory.createConnectionFactory(new ConnectionManagerAdapter());
        assertTrue( cf!=null );
        assertTrue( cf instanceof Serializable );
        assertTrue( cf instanceof Referenceable );
    }

    public void testImplementsQueueAndTopicConnectionFactory() throws Exception {
        Object cf = managedConnectionFactory.createConnectionFactory(new ConnectionManagerAdapter());
        assertTrue( cf instanceof QueueConnectionFactory );
        assertTrue( cf instanceof TopicConnectionFactory );
    }

}
