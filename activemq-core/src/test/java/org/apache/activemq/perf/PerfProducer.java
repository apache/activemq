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
package org.apache.activemq.perf;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * 
 */
public class PerfProducer implements Runnable {
    protected Connection connection;
    protected MessageProducer producer;
    protected PerfRate rate = new PerfRate();
    private final byte[] payload;
    private Session session;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private boolean running;
    private final boolean transacted;
    private int sleep = 0;

    public PerfProducer(ConnectionFactory fac, Destination dest, byte[] payload) throws JMSException {
        this(fac, dest, payload, false);
    }
    public PerfProducer(ConnectionFactory fac, Destination dest, byte[] payload, boolean transacted)
            throws JMSException {
        connection = fac.createConnection();
        this.transacted = transacted;
        if (transacted) {
            session = connection.createSession(true, Session.SESSION_TRANSACTED);
        } else {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }
        producer = session.createProducer(dest);
        this.payload = payload;
       
    }

    public void setDeliveryMode(int mode) throws JMSException {
        producer.setDeliveryMode(mode);
    }

    public void setTimeToLive(int ttl) throws JMSException {
        producer.setTimeToLive(ttl);
    }

    public void shutDown() throws JMSException {
        connection.close();
    }

    public PerfRate getRate() {
        return rate;
    }

    public synchronized void start() throws JMSException {
        if (!running) {
            rate.reset();
            running = true;
            connection.start();
            Thread t = new Thread(this);
            t.setName("Producer");
            t.start();
        }
    }

    public void stop() throws JMSException, InterruptedException {
        synchronized (this) {
            running = false;
        }
        stopped.await(1, TimeUnit.SECONDS);
        connection.stop();
    }

    public synchronized boolean isRunning() {
        return running;
    }

    public void run() {
        try {
            while (isRunning()) {
                BytesMessage msg;
                msg = session.createBytesMessage();
                msg.writeBytes(payload);
                producer.send(msg);
                if(this.transacted) {
                    this.session.commit();
                }
                rate.increment();
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            stopped.countDown();
        }
    }

    public int getSleep() {
        return sleep;
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }

}
