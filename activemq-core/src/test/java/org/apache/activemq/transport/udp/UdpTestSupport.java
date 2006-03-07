/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.transport.udp;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;

import java.io.IOException;

import junit.framework.TestCase;

/**
 *
 * @version $Revision$
 */
public abstract class UdpTestSupport extends TestCase implements TransportListener  {

    protected abstract Transport createConsumer() throws Exception;

    protected abstract Transport createProducer() throws Exception;

    protected Transport producer;
    protected Transport consumer;

    protected Object lock = new Object();
    protected Command receivedCommand;
    
    public void testSendingSmallMessage() throws Exception {
        ConsumerInfo expected = new ConsumerInfo();
        expected.setSelector("Cheese");
        try {
            producer.oneway(expected);
            
            Command received = assertCommandReceived();
            assertTrue("Should have received a ConsumerInfo but was: " + received, received instanceof ConsumerInfo);
            ConsumerInfo actual = (ConsumerInfo) received;
            assertEquals("Selector", expected.getSelector(), actual.getSelector());
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
            fail("Failed to send to transport: " + e);
        }
    }

    protected void setUp() throws Exception {
        consumer = createConsumer();
        producer = createProducer();
    
        consumer.setTransportListener(this);
        producer.setTransportListener(new TransportListener() {
            public void onCommand(Command command) {
            }
    
            public void onException(IOException error) {
            }
    
            public void transportInterupted() {
            }
    
            public void transportResumed() {
            }
        });
    
        consumer.start();
        producer.start();
    
    }

    protected void tearDown() throws Exception {
        if (producer != null) {
            producer.stop();
        }
        if (consumer != null) {
            consumer.stop();
        }
    }

    public void onCommand(Command command) {
        System.out.println("### Received command: " + command);
        
        synchronized (lock) {
            receivedCommand = command;
            lock.notifyAll();
        }
    }

    public void onException(IOException error) {
        System.out.println("### Received error: " + error);
    }

    public void transportInterupted() {
        System.out.println("### Transport interrupted");
    }

    public void transportResumed() {
        System.out.println("### Transport resumed");
    }


    protected Command assertCommandReceived() throws InterruptedException {
        Command answer = null;
        synchronized (lock) {
            lock.wait(5000);
            answer = receivedCommand;
        }
        
        assertNotNull("Should have received a Command by now!", answer);
        return answer;
    }

}
