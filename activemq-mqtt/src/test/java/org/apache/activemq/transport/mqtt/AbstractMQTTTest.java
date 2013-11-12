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
package org.apache.activemq.transport.mqtt;

import java.io.File;
import java.io.IOException;
import java.security.ProtectionDomain;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.AutoFailTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.ByteSequence;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

public abstract class AbstractMQTTTest extends AutoFailTestSupport {
    protected TransportConnector mqttConnector;
    protected TransportConnector openwireConnector;

    public static final int AT_MOST_ONCE =0;
    public static final int AT_LEAST_ONCE = 1;
    public static final int EXACTLY_ONCE =2;

    public File basedir() throws IOException {
        ProtectionDomain protectionDomain = getClass().getProtectionDomain();
        return new File(new File(protectionDomain.getCodeSource().getLocation().getPath()), "../..").getCanonicalFile();
    }

    protected BrokerService brokerService;
    protected LinkedList<Throwable> exceptions = new LinkedList<Throwable>();
    protected int numberOfMessages;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        exceptions.clear();
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setUseJmx(false);
        this.numberOfMessages = 1000;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
        super.tearDown();
    }

    protected String getProtocolScheme() {
        return "mqtt";
    }

    protected void addMQTTConnector() throws Exception {
        addMQTTConnector("");
    }

    protected void addMQTTConnector(String config) throws Exception {
        mqttConnector = brokerService.addConnector(getProtocolScheme()+"://localhost:0?" + config);
    }

    protected void addOpenwireConnector() throws Exception {
        openwireConnector = brokerService.addConnector("tcp://localhost:0");
    }

    protected void initializeConnection(MQTTClientProvider provider) throws Exception {
        provider.connect("tcp://localhost:"+mqttConnector.getConnectUri().getPort());
    }

    protected static interface Task {
        public void run() throws Exception;
    }

    protected  void within(int time, TimeUnit unit, Task task) throws InterruptedException {
        long timeMS = unit.toMillis(time);
        long deadline = System.currentTimeMillis() + timeMS;
        while (true) {
            try {
                task.run();
                return;
            } catch (Throwable e) {
                long remaining = deadline - System.currentTimeMillis();
                if( remaining <=0 ) {
                    if( e instanceof RuntimeException ) {
                        throw (RuntimeException)e;
                    }
                    if( e instanceof Error ) {
                        throw (Error)e;
                    }
                    throw new RuntimeException(e);
                }
                Thread.sleep(Math.min(timeMS/10, remaining));
            }
        }
    }

}