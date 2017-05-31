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
import java.io.*;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

    int messageCount = 1000;
    boolean runIndefinitely = false;
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
    AtomicInteger sentCount = new AtomicInteger(0);
    String message;
    String messageText = null;
    String payloadUrl = null;
    byte[] payload = null;
    boolean running = false;
    CountDownLatch finished;
    CountDownLatch paused = new CountDownLatch(0);


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

            if (runIndefinitely) {
                while (running) {
                    synchronized (this) {
                        paused.await();
                    }
                    sendMessage(producer, threadName);
                    sentCount.incrementAndGet();
                }
            }else{
                for (sentCount.set(0); sentCount.get() < messageCount && running; sentCount.incrementAndGet()) {
                    synchronized (this) {
                        paused.await();
                    }
                    sendMessage(producer, threadName);
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

    private void sendMessage(MessageProducer producer, String threadName) throws Exception {
        Message message = createMessage(sentCount.get());
        producer.send(message);
        if (LOG.isDebugEnabled()) {
            LOG.debug(threadName + " Sent: " + (message instanceof TextMessage ? ((TextMessage) message).getText() : message.getJMSMessageID()));
        }

        if (transactionBatchSize > 0 && sentCount.get() > 0 && sentCount.get() % transactionBatchSize == 0) {
            LOG.info(threadName + " Committing transaction: " + transactions++);
            session.commit();
        }

        if (sleep > 0) {
            Thread.sleep(sleep);
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
        Message answer;
        if (payload != null) {
            answer = session.createBytesMessage();
            ((BytesMessage) answer).writeBytes(payload);
        } else {
            if (textMessageSize > 0) {
                if (messageText == null) {
                    messageText = readInputStream(getClass().getResourceAsStream("demo.txt"), textMessageSize, i);
                }
            } else if (payloadUrl != null) {
                messageText = readInputStream(new URL(payloadUrl).openStream(), -1, i);
            } else if (message != null) {
                messageText = message;
            } else {
                messageText = createDefaultMessage(i);
            }
            answer = session.createTextMessage(messageText);
        }
        if ((msgGroupID != null) && (!msgGroupID.isEmpty())) {
            answer.setStringProperty("JMSXGroupID", msgGroupID);
        }
        return answer;
    }

    private String readInputStream(InputStream is, int size, int messageNumber) throws IOException {
        InputStreamReader reader = new InputStreamReader(is);
        try {
            char[] buffer;
            if (size > 0) {
                buffer = new char[size];
            } else {
                buffer = new char[1024];
            }
            int count;
            StringBuilder builder = new StringBuilder();
            while ((count = reader.read(buffer)) != -1) {
                builder.append(buffer, 0, count);
                if (size > 0) break;
            }
            return builder.toString();
        } catch (IOException ioe) {
            return createDefaultMessage(messageNumber);
        } finally {
            reader.close();
        }
    }

    private String createDefaultMessage(int messageNumber) {
        return "test message: " + messageNumber;
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
        return sentCount.get();
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

    public String getPayloadUrl() {
        return payloadUrl;
    }

    public void setPayloadUrl(String payloadUrl) {
        this.payloadUrl = payloadUrl;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isRunIndefinitely() {
        return runIndefinitely;
    }

    public void setRunIndefinitely(boolean runIndefinitely) {
        this.runIndefinitely = runIndefinitely;
    }

    public synchronized void pauseProducer(){
        this.paused = new CountDownLatch(1);
    }

    public synchronized void resumeProducer(){
        this.paused.countDown();
    }

    public void resetCounters(){
        this.sentCount.set(0);
    }
}
