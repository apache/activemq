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
package org.apache.activemq.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.CountDownLatch;

public class ProducerThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

    int messageCount = 1000;
    Destination destination;
    protected Session session;
    int sleep = 0;
    boolean persistent = true;
    int messageSize = 0;
    int textMessageSize;
    long msgTTL = 0L;
    String msgGroupID=null;
    int transactionBatchSize;

    int transactions = 0;
    int sentCount = 0;
    byte[] payload = null;
    boolean running = false;
    CountDownLatch finished;


    public ProducerThread(Session session, Destination destination) {
        this.destination = destination;
        this.session = session;
    }

    public void run() {
        MessageProducer producer = null;
        String threadName = Thread.currentThread().getName();
        try {
            producer = session.createProducer(destination);
            producer.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
            producer.setTimeToLive(msgTTL);
            initPayLoad();
            running = true;

            LOG.info(threadName +  " Started to calculate elapsed time ...\n");
            long tStart = System.currentTimeMillis();

            for (sentCount = 0; sentCount < messageCount && running; sentCount++) {
                Message message = createMessage(sentCount);
                producer.send(message);
                LOG.info(threadName + " Sent: " + (message instanceof TextMessage ? ((TextMessage) message).getText() : message.getJMSMessageID()));

                if (transactionBatchSize > 0 && sentCount > 0 && sentCount % transactionBatchSize == 0) {
                    LOG.info(threadName + " Committing transaction: " + transactions++);
                    session.commit();
                }

                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }

            LOG.info(threadName + " Produced: " + this.getSentCount() + " messages");
            long tEnd = System.currentTimeMillis();
            long elapsed = (tEnd - tStart) / 1000;
            LOG.info(threadName + " Elapsed time in second : " + elapsed + " s");
            LOG.info(threadName + " Elapsed time in milli second : " + (tEnd - tStart) + " milli seconds");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (finished != null) {
                finished.countDown();
            }
            if (producer != null) {
                try {
                    producer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void initPayLoad() {
        if (messageSize > 0) {
            payload = new byte[messageSize];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = '.';
            }
        }
    }

    protected Message createMessage(int i) throws Exception {
        Message message = null;
        if (payload != null) {
            message = session.createBytesMessage();
            ((BytesMessage)message).writeBytes(payload);
        } else {
            if (textMessageSize > 0) {
                InputStreamReader reader = null;
                try {
                    InputStream is = getClass().getResourceAsStream("demo.txt");
                    reader = new InputStreamReader(is);
                    char[] chars = new char[textMessageSize];
                    reader.read(chars);
                    message = session.createTextMessage(String.valueOf(chars));
                } catch (Exception e) {
                    LOG.warn(Thread.currentThread().getName() + " Failed to load " + textMessageSize + " bytes of demo text. Using default text message instead");
                    message = session.createTextMessage("test message: " + i);
                } finally {
                    if (reader != null) {
                        reader.close();
                    }
                }
            } else {
                message = session.createTextMessage("test message: " + i);
            }
        }
        if ((msgGroupID != null) && (!msgGroupID.isEmpty())) {
            message.setStringProperty("JMSXGroupID", msgGroupID);
        }
        return message;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public int getSleep() {
        return sleep;
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public int getSentCount() {
        return sentCount;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public long getMsgTTL() {
        return msgTTL;
    }

    public void setMsgTTL(long msgTTL) {
        this.msgTTL = msgTTL;
    }

    public int getTransactionBatchSize() {
        return transactionBatchSize;
    }

    public void setTransactionBatchSize(int transactionBatchSize) {
        this.transactionBatchSize = transactionBatchSize;
    }

    public String getMsgGroupID() {
        return msgGroupID;
    }

    public void setMsgGroupID(String msgGroupID) {
        this.msgGroupID = msgGroupID;
    }

    public int getTextMessageSize() {
        return textMessageSize;
    }

    public void setTextMessageSize(int textMessageSize) {
        this.textMessageSize = textMessageSize;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    public CountDownLatch getFinished() {
        return finished;
    }

    public void setFinished(CountDownLatch finished) {
        this.finished = finished;
    }
}
