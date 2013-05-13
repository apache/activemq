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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.bugs.embedded.ThreadExplorer;
import org.apache.activemq.network.NetworkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VmTransportNetworkBrokerTest extends TestCase {

	private static final Logger LOG = LoggerFactory.getLogger(VmTransportNetworkBrokerTest.class);

    private static final String VM_BROKER_URI = 
        "vm://localhost?create=false";
    
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch gotConnection = new CountDownLatch(1);

    public void testNoThreadLeak() throws Exception {

        // with VMConnection and simple discovery network connector
        int originalThreadCount = Thread.activeCount();
        LOG.debug(ThreadExplorer.show("threads at beginning"));
        
        BrokerService broker = new BrokerService();
        broker.setDedicatedTaskRunner(true);
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:61616");
        NetworkConnector networkConnector = broker.addNetworkConnector("static:(tcp://wrongHostname1:61617,tcp://wrongHostname2:61618)?useExponentialBackOff=false");
        networkConnector.setDuplex(true);
        broker.start();
        
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(new URI(VM_BROKER_URI));
        Connection connection = cf.createConnection("system", "manager");
        connection.start();
        
        // let it settle
        TimeUnit.SECONDS.sleep(5);
        
        int threadCountAfterStart = Thread.activeCount();
        TimeUnit.SECONDS.sleep(30);
        int threadCountAfterSleep = Thread.activeCount();
        
        assertTrue("Threads are leaking: " + ThreadExplorer.show("active sleep") + ", threadCount=" +threadCountAfterStart + " threadCountAfterSleep=" + threadCountAfterSleep,
                threadCountAfterSleep < threadCountAfterStart + 8);

        connection.close();
        broker.stop();
        broker.waitUntilStopped();

        // testNoDanglingThreadsAfterStop with tcp transport
        broker = new BrokerService();
        broker.setSchedulerSupport(true);
        broker.setDedicatedTaskRunner(true);
        broker.setPersistent(false);
        broker.addConnector("tcp://localhost:61616?wireFormat.maxInactivityDuration=1000&wireFormat.maxInactivityDurationInitalDelay=1000");
        broker.start();

        cf = new ActiveMQConnectionFactory("tcp://localhost:61616?wireFormat.maxInactivityDuration=1000&wireFormat.maxInactivityDurationInitalDelay=1000");
        connection = cf.createConnection("system", "manager");
        connection.start();
        connection.close();
        broker.stop();
        broker.waitUntilStopped();

        // let it settle
        TimeUnit.SECONDS.sleep(5);        

        // get final threads but filter out any daemon threads that the JVM may have created.
        Thread[] threads = filterDaemonThreads(ThreadExplorer.listThreads());
        int threadCountAfterStop = threads.length;

        // lets see the thread counts at INFO level so they are always in the test log
        LOG.info(ThreadExplorer.show("active after stop"));
        LOG.info("originalThreadCount=" + originalThreadCount + " threadCountAfterStop=" + threadCountAfterStop);

        assertTrue("Threads are leaking: " + 
        		ThreadExplorer.show("active after stop") + 
        		". originalThreadCount=" + 
        		originalThreadCount + 
        		" threadCountAfterStop=" + 
        		threadCountAfterStop,
            threadCountAfterStop <= originalThreadCount);
    }
    
    
    /**
     * Filters any daemon threads from the thread list.
     * 
     * Thread counts before and after the test should ideally be equal. 
     * However there is no guarantee that the JVM does not create any 
     * additional threads itself.
     * E.g. on Mac OSX there is a JVM internal thread called
     * "Poller SunPKCS11-Darwin" created after the test go started and 
     * under the main thread group.
     * When debugging tests in Eclipse another so called "Reader" thread 
     * is created by Eclipse.
     * So we cannot assume that the JVM does not create additional threads
     * during the test. However for the time being we assume that any such 
     * additionally created threads are daemon threads.
     *   
     * @param threads - the array of threads to parse
     * @return a new array with any daemon threads removed
     */
    public Thread[] filterDaemonThreads(Thread[] threads) throws Exception {
    
    	List<Thread> threadList = new ArrayList<Thread>(Arrays.asList(threads));
    	
    	// Can't use an Iterator as it would raise a 
    	// ConcurrentModificationException when trying to remove an element
    	// from the list, so using standard walk through
    	for (int i = 0 ; i < threadList.size(); i++) {
    		
    		Thread thread = threadList.get(i);
    		LOG.debug("Inspecting thread " + thread.getName());
    		if (thread.isDaemon()) {
    			LOG.debug("Removing deamon thread.");
    			threadList.remove(thread);
    			Thread.sleep(100);
    	
    		}
    	}
    	LOG.debug("Converting list back to Array");
    	return threadList.toArray(new Thread[0]);
    }
}
