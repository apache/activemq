/**
 *
 * Copyright 2004 Hiram Chirino
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.itests;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Hashtable;

import javax.ejb.CreateException;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import junit.framework.TestCase;

import org.activemq.itest.ejb.JMSToolHome;
import org.activemq.itest.ejb.JMSTool;

/**
 * @version $Revision: 1.1 $
 */
public class ResourceAdapterTest extends TestCase {

    private static final String JMSBEAN_JNDI = "org/activemq/itest/JMSToolBean";

    private static final String TEST_QUEUE = "TestQueue";

    private static final String TRANSFER_MDB_INPUT_QUEUE = "MDBInQueue";
    private static final String TRANSFER_MDB_OUTPUT_QUEUE = "MDBOutQueue";  

    
    private JMSTool jmsTool;

    
    protected void setUp() throws Exception {
        InitialContext ctx = createInitialContext();
        JMSToolHome home = (JMSToolHome) ctx.lookup(JMSBEAN_JNDI);
        jmsTool = home.create();
    }
    
    protected void tearDown() throws Exception {
        if( jmsTool != null ) {
            try {
                jmsTool.drain(TEST_QUEUE);
            } catch (Throwable e) {
            }
        }
    }
    
    public void testSendReceiveMultiple() throws CreateException, RemoteException, NamingException, JMSException {
        for( int i=0; i < 5; i++) {
            String msg1 = "Test Send Receive:"+i;
            jmsTool.sendTextMessage(TEST_QUEUE, msg1);
            String msg2 = jmsTool.receiveTextMessage(TEST_QUEUE, 1000);
            assertEquals("Message are not the same a iteration: "+i, msg1, msg2);
        }
    }

    /**
     * The MDBTransferBean should be moving message from the input queue to the output queue.
     * Check to see if message sent to it's input get to the output queue. 
     */
    public void testSendReceiveFromMDB() throws CreateException, RemoteException, NamingException, JMSException {
        HashSet a = new HashSet();
        HashSet b = new HashSet();
        
        for( int i=0; i < 5; i++) {
            String msg1 = "Test Send Receive From MDB:"+i;
            a.add(msg1);
            jmsTool.sendTextMessage(TRANSFER_MDB_INPUT_QUEUE, msg1);
        }
        
        for( int i=0; i < 5; i++) {
            String msg2 = jmsTool.receiveTextMessage(TRANSFER_MDB_OUTPUT_QUEUE, 1000);
            b.add(msg2);
        }
        
        // Compare the messages using sets since they may be received out of order since,
        // the MDB runns concurrent threads.
        
        assertEquals(a,b);
    }
    
    private InitialContext createInitialContext() throws NamingException {
        Hashtable props = new Hashtable();
        props.put("java.naming.factory.initial", "org.openejb.client.RemoteInitialContextFactory");
        props.put("java.naming.provider.url", "127.0.0.1:4201");
        props.put("java.naming.security.principal", "testuser");
        props.put("java.naming.security.credentials", "testpassword");
        return new InitialContext(props);
    }

}
