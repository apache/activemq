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
package org.apache.activemq.tool;

import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple container of messages for performing testing and rendezvous style
 * code. You can use this class a {@link MessageListener} and then make
 * assertions about how many messages it has received allowing a certain maximum
 * amount of time to ensure that the test does not hang forever.
 * <p/>
 * Also you can chain these instances together with the
 * {@link #setParent(MessageListener)} method so that you can aggregate the
 * total number of messages consumed across a number of consumers.
 *
 * @version $Revision: 1.6 $
 */
public class MemMessageIdList implements MessageListener {

    protected static final Log log = LogFactory.getLog(MemMessageIdList.class);

    private List messageIds = new ArrayList();
    private Object semaphore;
    private boolean verbose;
    private MessageListener parent;
    private long maximumDuration = 15000L;

    public MemMessageIdList() {
        this(new Object());
    }

    public MemMessageIdList(Object semaphore) {
        this.semaphore = semaphore;
    }

    public boolean equals(Object that) {
        if (that instanceof MemMessageIdList) {
            MemMessageIdList thatListMem = (MemMessageIdList) that;
            return getMessageIds().equals(thatListMem.getMessageIds());
        }
        return false;
    }

    public int hashCode() {
        synchronized (semaphore) {
            return messageIds.hashCode() + 1;
        }
    }

    public String toString() {
        synchronized (semaphore) {
            return messageIds.toString();
        }
    }

    /**
     * @return all the messages on the list so far, clearing the buffer
     */
    public List flushMessages() {
        synchronized (semaphore) {
            List answer = new ArrayList(messageIds);
            messageIds.clear();
            return answer;
        }
    }

    public synchronized List getMessageIds() {
        synchronized (semaphore) {
            return new ArrayList(messageIds);
        }
    }

    public void onMessage(Message message) {
        String id = null;
        try {
            id = message.getJMSMessageID();
            synchronized (semaphore) {
                messageIds.add(id);
                semaphore.notifyAll();
            }
            if (verbose) {
                log.info("Received message: " + message);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        if (parent != null) {
            parent.onMessage(message);
        }
    }

    public int getMessageCount() {
        synchronized (semaphore) {
            return messageIds.size();
        }
    }

    public void waitForMessagesToArrive(int messageCount) {
        log.info("Waiting for " + messageCount + " message(s) to arrive");

        long start = System.currentTimeMillis();

        for (int i = 0; i < messageCount; i++) {
            try {
                if (hasReceivedMessages(messageCount)) {
                    break;
                }
                long duration = System.currentTimeMillis() - start;
                if (duration >= maximumDuration) {
                    break;
                }
                synchronized (semaphore) {
                    semaphore.wait(maximumDuration - duration);
                }
            } catch (InterruptedException e) {
                log.info("Caught: " + e);
            }
        }
        long end = System.currentTimeMillis() - start;

        log.info("End of wait for " + end + " millis and received: " + getMessageCount() + " messages");
    }


    public boolean hasReceivedMessage() {
        return getMessageCount() == 0;
    }

    public boolean hasReceivedMessages(int messageCount) {
        return getMessageCount() >= messageCount;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public MessageListener getParent() {
        return parent;
    }

    /**
     * Allows a parent listener to be specified such as to aggregate messages
     * consumed across consumers
     */
    public void setParent(MessageListener parent) {
        this.parent = parent;
    }

}
