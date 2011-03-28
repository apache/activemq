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
package org.apache.activemq.transport.tcp;

import java.util.Timer;
import java.util.TimerTask;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;

public class TcpTransportBindTest extends EmbeddedBrokerTestSupport {
    final String addr = "tcp://localhost:61617";
    
    /**
     * exercise some server side socket options
     * @throws Exception
     */
    protected void setUp() throws Exception {   
        bindAddress = addr + "?transport.reuseAddress=true&transport.soTimeout=1000";
        super.setUp();
    }
    
    public void testConnect() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(addr).createConnection();
        connection.start();
    }
    
    
    public void testReceiveThrowsException() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(addr).createConnection();
        connection.start();
        Session sess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = sess.createConsumer(createDestination());
        class StopTask extends TimerTask {
            public void run() {
                try {
                    broker.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        Timer timer = new Timer();
        timer.schedule(new StopTask(), 1000);
        try {
            consumer.receive(30000);
            fail("Should have thrown an exception");
        } catch (Exception e) {
            // should fail
        }
    }
}
