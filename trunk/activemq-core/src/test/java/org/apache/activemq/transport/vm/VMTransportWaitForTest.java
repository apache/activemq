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
package org.apache.activemq.transport.vm;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

public class VMTransportWaitForTest extends TestCase {

    private static final String VM_BROKER_URI_NO_WAIT = 
        "vm://localhost?broker.persistent=false&create=false";
    
    private static final String VM_BROKER_URI_WAIT_FOR_START = 
        VM_BROKER_URI_NO_WAIT + "&waitForStart=20000";
    
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch gotConnection = new CountDownLatch(1);

    public void testWaitFor() throws Exception {
        try {
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(new URI(VM_BROKER_URI_NO_WAIT));
            cf.createConnection();
            fail("expect broker not exist exception");
        } catch (JMSException expectedOnNoBrokerAndNoCreate) {
        }
        
        // spawn a thread that will wait for an embedded broker to start via vm://..
        Thread t = new Thread() {
            public void run() {
                    try {
                        started.countDown();
                        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(new URI(VM_BROKER_URI_WAIT_FOR_START));
                        cf.createConnection();
                        gotConnection.countDown();
                   
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("unexpected exception: " + e);
                    }
            }
        };
        t.start();
        started.await(20, TimeUnit.SECONDS);
        Thread.yield();
        assertFalse("has not got connection", gotConnection.await(2, TimeUnit.SECONDS));
        
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:61616");
        broker.start();
        assertTrue("has got connection", gotConnection.await(400, TimeUnit.MILLISECONDS));
        broker.stop(); 
    }
}
