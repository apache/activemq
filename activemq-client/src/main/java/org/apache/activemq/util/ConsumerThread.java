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
import java.util.concurrent.CountDownLatch;

public class ConsumerThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);

    int messageCount = 1000;
    int receiveTimeOut = 3000;
    Destination destination;
    Session session;
    boolean durable;
    boolean breakOnNull = true;
    int sleep;
    int batchSize;

    int received = 0;
    int transactions = 0;
    boolean running = false;
    CountDownLatch finished;
    boolean bytesAsText;

    public ConsumerThread(Session session, Destination destination) {
        this.destination = destination;
        this.session = session;
    }

    @Override
    public void run() {
        running = true;
        MessageConsumer consumer = null;
        String threadName = Thread.currentThread().getName();
        LOG.info(threadName + " wait until " + messageCount + " messages are consumed");
        try {
            if (durable && destination instanceof Topic) {
                consumer = session.createDurableSubscriber((Topic) destination, getName());
            } else {
                consumer = session.createConsumer(destination);
            }
            while (running && received < messageCount) {
                Message msg = consumer.receive(receiveTimeOut);
                if (msg != null) {
                    LOG.info(threadName + " Received " + (msg instanceof TextMessage ? ((TextMessage) msg).getText() : msg.getJMSMessageID()));
                    if (bytesAsText && (msg instanceof BytesMessage)) {
                        long length = ((BytesMessage) msg).getBodyLength();
                        byte[] bytes = new byte[(int) length];
                        ((BytesMessage) msg).readBytes(bytes);
                        LOG.info("BytesMessage as text string: " + new String(bytes));
                    }
                    received++;
                } else {
                    if (breakOnNull) {
                        break;
                    }
                }

                if (session.getTransacted()) {
                    if (batchSize > 0 && received > 0 && received % batchSize == 0) {
                        LOG.info(threadName + " Committing transaction: " + transactions++);
                        session.commit();
                    }
                } else if (session.getAcknowledgeMode() == Session.CLIENT_ACKNOWLEDGE) {
                    if (batchSize > 0 && received > 0 && received % batchSize == 0) {
                        LOG.info("Acknowledging last " + batchSize + " messages; messages so far = " + received);
                        msg.acknowledge();
                    }
                }
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (finished != null) {
                finished.countDown();
            }
            if (consumer != null) {
                LOG.info(threadName + " Consumed: " + this.getReceived() + " messages");
                try {
                    consumer.close();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }

        LOG.info(threadName + " Consumer thread finished");
    }

    public int getReceived() {
        return received;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public void setMessageCount(int messageCount) {
        this.messageCount = messageCount;
    }

    public void setBreakOnNull(boolean breakOnNull) {
        this.breakOnNull = breakOnNull;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public boolean isBreakOnNull() {
        return breakOnNull;
    }

    public int getReceiveTimeOut() {
        return receiveTimeOut;
    }

    public void setReceiveTimeOut(int receiveTimeOut) {
        this.receiveTimeOut = receiveTimeOut;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public int getSleep() {
        return sleep;
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }

    public CountDownLatch getFinished() {
        return finished;
    }

    public void setFinished(CountDownLatch finished) {
        this.finished = finished;
    }

    public boolean isBytesAsText() {
        return bytesAsText;
    }

    public void setBytesAsText(boolean bytesAsText) {
        this.bytesAsText = bytesAsText;
    }
}
