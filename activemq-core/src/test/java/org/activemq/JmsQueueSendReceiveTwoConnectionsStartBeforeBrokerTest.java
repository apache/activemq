/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq;

import edu.emory.mathcs.backport.java.util.Queue;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentLinkedQueue;

import org.activemq.broker.BrokerService;


/**
 * @version $Revision$
 */
public class JmsQueueSendReceiveTwoConnectionsStartBeforeBrokerTest extends JmsQueueSendReceiveTwoConnectionsTest {

    private Queue errors = new ConcurrentLinkedQueue();
    private int delayBeforeStartingBroker = 1000;
    private BrokerService broker;

    public void startBroker() {
        // Initialize the broker
        System.out.println("Lets wait: " + delayBeforeStartingBroker + " millis  before creating the broker");
        try {
            Thread.sleep(delayBeforeStartingBroker);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        System.out.println("Now starting the broker");
        try {
            broker = new BrokerService();
            broker.setPersistent(false);
            broker.addConnector("tcp://localhost:61616");
            broker.start();
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            errors.add(e);
        }
    }
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("failover:(tcp://localhost:61616)?maxReconnectAttempts=10&useExponentialBackOff=false&initialReconnectDelay=200");
    }

    protected void setUp() throws Exception {
        // now lets asynchronously start a broker
        Thread thread = new Thread() {
            public void run() {
                startBroker();
            }
        };
        thread.start();

        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        if (broker != null) {
            broker.stop();
        }
        if (!errors.isEmpty()) {
            Exception e = (Exception) errors.remove();
            throw e;
        }
    }
    
    

}
