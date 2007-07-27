/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple container of messages for performing testing and rendezvous style
 * code. You can use this class a {@link MessageListener} and then make
 * assertions about how many messages it has received allowing a certain maximum
 * amount of time to ensure that the test does not hang forever.
 * 
 * Also you can chain these instances together with the
 * {@link #setParent(MessageListener)} method so that you can aggregate the
 * total number of messages consumed across a number of consumers.
 * 
 * @version $Revision: 1.6 $
 */
public class MessageIdList extends Assert implements MessageListener {
    
    protected static final Log log = LogFactory.getLog(MessageIdList.class);

    private List messageIds = new ArrayList();
    private Object semaphore;
    private boolean verbose;
    private MessageListener parent;
    private long maximumDuration = 15000L;
    private long processingDelay=0;
    
	private CountDownLatch countDownLatch;

    public MessageIdList() {
        this(new Object());
    }

    public MessageIdList(Object semaphore) {
        this.semaphore = semaphore;
    }

    public boolean equals(Object that) {
        if (that instanceof MessageIdList) {
            MessageIdList thatList = (MessageIdList) that;
            return getMessageIds().equals(thatList.getMessageIds());
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
        String id=null;
        try {
        	if( countDownLatch != null )
        		countDownLatch.countDown();
        	
            id = message.getJMSMessageID();
            synchronized (semaphore) {
                messageIds.add(id);
                semaphore.notifyAll();
            }
            if (log.isDebugEnabled()) {
                log.debug("Received message: " + message);
            }
        } catch (JMSException e) {
            e.printStackTrace();
        }
        if (parent != null) {
            parent.onMessage(message);
        }
        if( processingDelay > 0 ) {
        	try {
				Thread.sleep(processingDelay);
			} catch (InterruptedException e) {
			}
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
                if (duration >= maximumDuration ) {
                    break;
                }
                synchronized (semaphore) {
                    semaphore.wait(maximumDuration-duration);
                }
            }
            catch (InterruptedException e) {
                log.info("Caught: " + e);
            }
        }
        long end = System.currentTimeMillis() - start;

        log.info("End of wait for " + end + " millis and received: " + getMessageCount() + " messages");
    }

    /**
     * Performs a testing assertion that the correct number of messages have
     * been received without waiting
     * 
     * @param messageCount
     */
    public void assertMessagesReceivedNoWait(int messageCount) {
        assertEquals("expected number of messages when received", messageCount, getMessageCount());
    }
    
    /**
     * Performs a testing assertion that the correct number of messages have
     * been received waiting for the messages to arrive up to a fixed amount of time.
     * 
     * @param messageCount
     */
    public void assertMessagesReceived(int messageCount) {
        waitForMessagesToArrive(messageCount);

        assertMessagesReceivedNoWait(messageCount);
    }

    /**
     * Asserts that there are at least the given number of messages received without waiting.
     */
    public void assertAtLeastMessagesReceived(int messageCount) {
        int actual = getMessageCount();
        assertTrue("at least: " + messageCount + " messages received. Actual: " + actual, actual >= messageCount);
    }

    /**
     * Asserts that there are at most the number of messages received without waiting
     * @param messageCount
     */
    public void assertAtMostMessagesReceived(int messageCount) {
        int actual = getMessageCount();
        assertTrue("at most: " + messageCount + " messages received. Actual: " + actual, actual <= messageCount);
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

    
    /**
     * @return the maximumDuration
     */
    public long getMaximumDuration(){
        return this.maximumDuration;
    }

    
    /**
     * @param maximumDuration the maximumDuration to set
     */
    public void setMaximumDuration(long maximumDuration){
        this.maximumDuration=maximumDuration;
    }

	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

	/**
	 * Gets the amount of time the message listener will spend sleeping to
	 * simulate a processing delay.
	 * 
	 * @return
	 */
	public long getProcessingDelay() {
		return processingDelay;
	}

	/**
	 * Sets the amount of time the message listener will spend sleeping to
	 * simulate a processing delay.
	 * 
	 * @param processingDelay
	 */
	public void setProcessingDelay(long processingDelay) {
		this.processingDelay = processingDelay;
	}

}
