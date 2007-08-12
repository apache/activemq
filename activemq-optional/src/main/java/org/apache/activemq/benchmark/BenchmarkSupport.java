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
package org.apache.activemq.benchmark;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.util.IdGenerator;

/**
 * Abstract base class for some simple benchmark tools
 * 
 * @author James Strachan
 * @version $Revision$
 */
public class BenchmarkSupport {

    protected int connectionCount = 1;
    protected int batch = 1000;
    protected Destination destination;
    protected String[] subjects;

    private boolean topic = true;
    private boolean durable;
    private ActiveMQConnectionFactory factory;
    private String url;
    private int counter;
    private List<Object> resources = new ArrayList<Object>();
    private NumberFormat formatter = NumberFormat.getInstance();
    private AtomicInteger connectionCounter = new AtomicInteger(0);
    private IdGenerator idGenerator = new IdGenerator();

    public BenchmarkSupport() {
    }

    public void start() {
        System.out.println("Using: " + connectionCount + " connection(s)");
        subjects = new String[connectionCount];
        for (int i = 0; i < connectionCount; i++) {
            subjects[i] = "BENCHMARK.FEED" + i;
        }
        if (useTimerLoop()) {
            Thread timer = new Thread() {
                public void run() {
                    timerLoop();
                }
            };
            timer.start();
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean isTopic() {
        return topic;
    }

    public void setTopic(boolean topic) {
        this.topic = topic;
    }

    public ActiveMQConnectionFactory getFactory() {
        return factory;
    }

    public void setFactory(ActiveMQConnectionFactory factory) {
        this.factory = factory;
    }

    public void setSubject(String subject) {
        connectionCount = 1;
        subjects = new String[] {
            subject
        };
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public int getConnectionCount() {
        return connectionCount;
    }

    public void setConnectionCount(int connectionCount) {
        this.connectionCount = connectionCount;
    }

    protected Session createSession() throws JMSException {
        if (factory == null) {
            factory = createFactory();
        }
        Connection connection = factory.createConnection();
        int value = connectionCounter.incrementAndGet();
        System.out.println("Created connection: " + value + " = " + connection);
        if (durable) {
            connection.setClientID(idGenerator.generateId());
        }
        addResource(connection);
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        addResource(session);
        return session;
    }

    protected ActiveMQConnectionFactory createFactory() {
        ActiveMQConnectionFactory answer = new ActiveMQConnectionFactory(getUrl());
        return answer;
    }

    protected synchronized void count(int count) {
        counter += count;
        /*
         * if (counter > batch) { counter = 0; long current =
         * System.currentTimeMillis(); double end = current - time; end /= 1000;
         * time = current; System.out.println("Processed " + batch + " messages
         * in " + end + " (secs)"); }
         */
    }

    protected synchronized int resetCount() {
        int answer = counter;
        counter = 0;
        return answer;
    }

    protected void timerLoop() {
        int times = 0;
        int total = 0;
        int dumpVmStatsFrequency = 10;
        Runtime runtime = Runtime.getRuntime();

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int processed = resetCount();
            double average = 0;
            if (processed > 0) {
                total += processed;
                times++;
            }
            if (times > 0) {
                average = total / times;
            }

            System.out.println(getClass().getName() + " Processed: " + processed + " messages this second. Average: " + average);

            if ((times % dumpVmStatsFrequency) == 0 && times != 0) {
                System.out.println("Used memory: " + asMemoryString(runtime.totalMemory() - runtime.freeMemory()) + " Free memory: " + asMemoryString(runtime.freeMemory()) + " Total memory: "
                                   + asMemoryString(runtime.totalMemory()) + " Max memory: " + asMemoryString(runtime.maxMemory()));
            }

        }
    }

    protected String asMemoryString(long value) {
        return formatter.format(value / 1024) + " K";
    }

    protected boolean useTimerLoop() {
        return true;
    }

    protected Destination createDestination(Session session, String subject) throws JMSException {
        if (topic) {
            return session.createTopic(subject);
        } else {
            return session.createQueue(subject);
        }
    }

    protected void addResource(Object resource) {
        resources.add(resource);
    }

    protected static boolean parseBoolean(String text) {
        return text.equalsIgnoreCase("true");
    }
}
