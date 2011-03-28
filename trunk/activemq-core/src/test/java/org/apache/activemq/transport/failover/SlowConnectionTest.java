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
package org.apache.activemq.transport.failover;

import java.io.IOException;
import java.net.*;
import java.util.*;

import javax.jms.Connection;
import javax.net.ServerSocketFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;

public class SlowConnectionTest extends TestCase {
        
    public void testSlowConnection() throws Exception {
        
        int timeout = 1000;
        URI tcpUri = new URI("tcp://localhost:61616?soTimeout=" + timeout + "&trace=true&connectionTimeout=" + timeout + "&wireFormat.maxInactivityDurationInitalDelay=" + timeout);
        
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")");
        final Connection connection = cf.createConnection();
        
        MockBroker broker = new MockBroker();
        broker.start();
        
        new Thread(new Runnable() {
            public void run() {
                try { connection.start(); } catch (Throwable ignored) {}
            }
        }).start();
        
        Thread.sleep(timeout * 5);
        
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().contains("ActiveMQ Transport")) { count++; }
        }
        
        broker.interrupt();
        broker.join();
        
        assertTrue("Transport count: " + count + ", expected <= 1", count <= 1);        
    }   
    
    class MockBroker extends Thread {
        
        public void run() {
            
            List<Socket> inProgress = new ArrayList<Socket>();
            ServerSocketFactory factory = ServerSocketFactory.getDefault();
            ServerSocket ss = null;
            
            try {
                ss = factory.createServerSocket(61616);
                
                while (!interrupted()) {
                    inProgress.add(ss.accept());    // eat socket
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try { ss.close(); } catch (IOException ignored) {}
                for (Socket s : inProgress) {               
                    try { s.close(); } catch (IOException ignored) {}
                }               
            }           
        }
    }
}

