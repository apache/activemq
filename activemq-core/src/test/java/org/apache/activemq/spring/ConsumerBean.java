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
package org.apache.activemq.spring;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Message;
import javax.jms.MessageListener;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ConsumerBean extends Assert implements MessageListener {
    private static final Log LOG = LogFactory.getLog(ConsumerBean.class);
    private List<Message> messages = new ArrayList<Message>();
    private Object semaphore;
    private boolean verbose;

    /**
     * Constructor.
     */
    public ConsumerBean() {
        this(new Object());
    }

    /**
     * Constructor, initialized semaphore object.
     * 
     * @param semaphore
     */
    public ConsumerBean(Object semaphore) {
        this.semaphore = semaphore;
    }

    /**
     * @return all the messages on the list so far, clearing the buffer
     */
    public synchronized List<Message> flushMessages() {
        List<Message> answer = new ArrayList<Message>(messages);
        messages.clear();
        return answer;
    }

    /**
     * Method implemented from MessageListener interface.
     * 
     * @param message
     */
    public synchronized void onMessage(Message message) {
        messages.add(message);
        if (verbose) {
            LOG.info("Received: " + message);
        }
        synchronized (semaphore) {
            semaphore.notifyAll();
        }
    }

    /**
     * Use to wait for a single message to arrive.
     */
    public void waitForMessageToArrive() {
        LOG.info("Waiting for message to arrive");

        long start = System.currentTimeMillis();

        try {
            if (hasReceivedMessage()) {
                synchronized (semaphore) {
                    semaphore.wait(4000);
                }
            }
        } catch (InterruptedException e) {
            LOG.info("Caught: " + e);
        }
        long end = System.currentTimeMillis() - start;

        LOG.info("End of wait for " + end + " millis");
    }

    /**
     * Used to wait for a message to arrive given a particular message count.
     * 
     * @param messageCount
     */
    public void waitForMessagesToArrive(int messageCount) {
        LOG.info("Waiting for message to arrive");

        long start = System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            try {
                if (hasReceivedMessages(messageCount)) {
                    break;
                }
                synchronized (semaphore) {
                    semaphore.wait(1000);
                }
            } catch (InterruptedException e) {
                LOG.info("Caught: " + e);
            }
        }
        long end = System.currentTimeMillis() - start;

        LOG.info("End of wait for " + end + " millis");
    }

    public void assertMessagesArrived(int total) {
        waitForMessagesToArrive(total);
        synchronized (this) {
            int count = messages.size();

            assertEquals("Messages received", total, count);
        }
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * Identifies if the message is empty.
     * 
     * @return
     */
    protected boolean hasReceivedMessage() {
        return messages.isEmpty();
    }

    /**
     * Identifies if the message count has reached the total size of message.
     * 
     * @param messageCount
     * @return
     */
    protected synchronized boolean hasReceivedMessages(int messageCount) {
        return messages.size() >= messageCount;
    }
}
