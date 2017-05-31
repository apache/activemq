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

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerBean extends Assert implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerBean.class);
    private final List<Message> messages = new ArrayList<Message>();
    private boolean verbose;
    private String id = null;

    /**
     * Constructor.
     */
    public ConsumerBean() {
    }

    public ConsumerBean(String id) {
        this.id = id;
    }

    /**
     * @return all the messages on the list so far, clearing the buffer
     */
    public List<Message> flushMessages() {
        List<Message> answer = null;
        synchronized(messages) {
        answer = new ArrayList<Message>(messages);
        messages.clear();
        }
        return answer;
    }

    /**
     * Method implemented from MessageListener interface.
     *
     * @param message
     */
    @Override
    public void onMessage(Message message) {
        synchronized (messages) {
            messages.add(message);
            if (verbose) {
                LOG.info("" + id + "Received: " + message);
            }
            messages.notifyAll();
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
                synchronized (messages) {
                    messages.wait(4000);
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

    public void waitForMessagesToArrive(int messageCount){
        waitForMessagesToArrive(messageCount,120 * 1000);
    }
    public void waitForMessagesToArrive(int messageCount,long maxWaitTime) {
        long maxRemainingMessageCount = Math.max(0, messageCount - messages.size());
        LOG.info("Waiting for (" + maxRemainingMessageCount + ") message(s) to arrive");
        long start = System.currentTimeMillis();
        long endTime = start + maxWaitTime;
        while (maxRemainingMessageCount > 0) {
            try {
                synchronized (messages) {
                    messages.wait(1000);
                }
                if (hasReceivedMessages(messageCount) || System.currentTimeMillis() > endTime) {
                    break;
                }
            } catch (InterruptedException e) {
                LOG.info("Caught: " + e);
            }
            maxRemainingMessageCount = Math.max(0, messageCount - messages.size());
        }
        long end = System.currentTimeMillis() - start;
        LOG.info("End of wait for " + end + " millis");
    }

    public void assertMessagesArrived(int total) {
        waitForMessagesToArrive(total);
        synchronized (messages) {
            int count = messages.size();

            assertEquals("Messages received", total, count);
        }
    }

    public void assertMessagesArrived(int total, long maxWaitTime) {
        waitForMessagesToArrive(total,maxWaitTime);
        synchronized (messages) {
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

    public List<Message> getMessages() {
        return messages;
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
    protected boolean hasReceivedMessages(int messageCount) {
        synchronized (messages) {
            return messages.size() >= messageCount;
        }
    }
}
