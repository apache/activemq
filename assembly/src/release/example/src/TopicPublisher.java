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
import java.util.Arrays;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Use in conjunction with TopicListener to test the performance of ActiveMQ
 * Topics.
 */
public class TopicPublisher implements MessageListener {

    private static final char[] DATA = "abcdefghijklmnopqrstuvwxyz".toCharArray();

    private final Object mutex = new Object();
    private Connection connection;
    private Session session;
    private MessageProducer publisher;
    private Topic topic;
    private Topic control;

    private String url = "tcp://localhost:61616";
    private int size = 256;
    private int subscribers = 1;
    private int remaining;
    private int messages = 10000;
    private long delay;
    private int batch = 40;

    private byte[] payload;

    public static void main(String[] argv) throws Exception {
        TopicPublisher p = new TopicPublisher();
        String[] unknown = CommandLineSupport.setOptions(p, argv);
        if (unknown.length > 0) {
            System.out.println("Unknown options: " + Arrays.toString(unknown));
            System.exit(-1);
        }
        p.run();
    }

    private void run() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        topic = session.createTopic("topictest.messages");
        control = session.createTopic("topictest.control");

        publisher = session.createProducer(topic);
        publisher.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        payload = new byte[size];
        for (int i = 0; i < size; i++) {
            payload[i] = (byte)DATA[i % DATA.length];
        }

        session.createConsumer(control).setMessageListener(this);
        connection.start();

        long[] times = new long[batch];
        for (int i = 0; i < batch; i++) {
            if (i > 0) {
                Thread.sleep(delay * 1000);
            }
            times[i] = batch(messages);
            System.out.println("Batch " + (i + 1) + " of " + batch + " completed in " + times[i] + " ms.");
        }

        long min = min(times);
        long max = max(times);
        System.out.println("min: " + min + ", max: " + max + " avg: " + avg(times, min, max));

        // request shutdown
        publisher.send(session.createTextMessage("SHUTDOWN"));

        connection.stop();
        connection.close();
    }

    private long batch(int msgCount) throws Exception {
        long start = System.currentTimeMillis();
        remaining = subscribers;
        publish();
        waitForCompletion();
        return System.currentTimeMillis() - start;
    }

    private void publish() throws Exception {

        // send events
        BytesMessage msg = session.createBytesMessage();
        msg.writeBytes(payload);
        for (int i = 0; i < messages; i++) {
            publisher.send(msg);
            if ((i + 1) % 1000 == 0) {
                System.out.println("Sent " + (i + 1) + " messages");
            }
        }

        // request report
        publisher.send(session.createTextMessage("REPORT"));
    }

    private void waitForCompletion() throws Exception {
        System.out.println("Waiting for completion...");
        synchronized (mutex) {
            while (remaining > 0) {
                mutex.wait();
            }
        }
    }

    public void onMessage(Message message) {
        synchronized (mutex) {
            System.out.println("Received report " + getReport(message) + " " + --remaining + " remaining");
            if (remaining == 0) {
                mutex.notify();
            }
        }
    }

    Object getReport(Message m) {
        try {
            return ((TextMessage)m).getText();
        } catch (JMSException e) {
            e.printStackTrace(System.out);
            return e.toString();
        }
    }

    static long min(long[] times) {
        long min = times.length > 0 ? times[0] : 0;
        for (int i = 0; i < times.length; i++) {
            min = Math.min(min, times[i]);
        }
        return min;
    }

    static long max(long[] times) {
        long max = times.length > 0 ? times[0] : 0;
        for (int i = 0; i < times.length; i++) {
            max = Math.max(max, times[i]);
        }
        return max;
    }

    static long avg(long[] times, long min, long max) {
        long sum = 0;
        for (int i = 0; i < times.length; i++) {
            sum += times[i];
        }
        sum -= min;
        sum -= max;
        return sum / times.length - 2;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public void setMessages(int messages) {
        this.messages = messages;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void setSubscribers(int subscribers) {
        this.subscribers = subscribers;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
