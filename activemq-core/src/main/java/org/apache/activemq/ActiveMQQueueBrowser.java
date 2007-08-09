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
package org.apache.activemq;

import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;

/**
 * A client uses a <CODE>QueueBrowser</CODE> object to look at messages on a
 * queue without removing them. <p/>
 * <P>
 * The <CODE>getEnumeration</CODE> method returns a <CODE>
 * java.util.Enumeration</CODE>
 * that is used to scan the queue's messages. It may be an enumeration of the
 * entire content of a queue, or it may contain only the messages matching a
 * message selector. <p/>
 * <P>
 * Messages may be arriving and expiring while the scan is done. The JMS API
 * does not require the content of an enumeration to be a static snapshot of
 * queue content. Whether these changes are visible or not depends on the JMS
 * provider. <p/>
 * <P>
 * A <CODE>QueueBrowser</CODE> can be created from either a <CODE>Session
 * </CODE>
 * or a <CODE>QueueSession</CODE>.
 * 
 * @see javax.jms.Session#createBrowser
 * @see javax.jms.QueueSession#createBrowser
 * @see javax.jms.QueueBrowser
 * @see javax.jms.QueueReceiver
 */

public class ActiveMQQueueBrowser implements QueueBrowser, Enumeration {

    private final ActiveMQSession session;
    private final ActiveMQDestination destination;
    private final String selector;

    private ActiveMQMessageConsumer consumer;
    private boolean closed;
    private final ConsumerId consumerId;
    private final AtomicBoolean browseDone = new AtomicBoolean(true);
    private final boolean dispatchAsync;
    private Object semaphore = new Object();

    /**
     * Constructor for an ActiveMQQueueBrowser - used internally
     * 
     * @param theSession
     * @param dest
     * @param selector
     * @throws JMSException
     */
    protected ActiveMQQueueBrowser(ActiveMQSession session, ConsumerId consumerId, ActiveMQDestination destination, String selector, boolean dispatchAsync) throws JMSException {
        this.session = session;
        this.consumerId = consumerId;
        this.destination = destination;
        this.selector = selector;
        this.dispatchAsync = dispatchAsync;
        this.consumer = createConsumer();
    }

    /**
     * @param session
     * @param originalDestination
     * @param selectorExpression
     * @param cnum
     * @return
     * @throws JMSException
     */
    private ActiveMQMessageConsumer createConsumer() throws JMSException {
        browseDone.set(false);
        ActiveMQPrefetchPolicy prefetchPolicy = session.connection.getPrefetchPolicy();
        return new ActiveMQMessageConsumer(session, consumerId, destination, null, selector, prefetchPolicy.getQueueBrowserPrefetch(), prefetchPolicy
            .getMaximumPendingMessageLimit(), false, true, dispatchAsync) {
            public void dispatch(MessageDispatch md) {
                if (md.getMessage() == null) {
                    browseDone.set(true);
                } else {
                    super.dispatch(md);
                }
                notifyMessageAvailable();
            }
        };
    }

    private void destroyConsumer() {
        if (consumer == null)
            return;
        try {
            consumer.close();
            consumer = null;
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets an enumeration for browsing the current queue messages in the order
     * they would be received.
     * 
     * @return an enumeration for browsing the messages
     * @throws JMSException if the JMS provider fails to get the enumeration for
     *                 this browser due to some internal error.
     */

    public Enumeration getEnumeration() throws JMSException {
        checkClosed();
        if (consumer == null)
            consumer = createConsumer();
        return this;
    }

    private void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("The Consumer is closed");
        }
    }

    /**
     * @return true if more messages to process
     */
    public boolean hasMoreElements() {
        while (true) {

            synchronized (this) {
                if (consumer == null)
                    return false;
            }

            if (consumer.getMessageSize() > 0) {
                return true;
            }

            if (browseDone.get() || !session.isRunning()) {
                destroyConsumer();
                return false;
            }

            waitForMessage();
        }
    }

    /**
     * @return the next message
     */
    public Object nextElement() {
        while (true) {

            synchronized (this) {
                if (consumer == null)
                    return null;
            }

            try {
                Message answer = consumer.receiveNoWait();
                if (answer != null)
                    return answer;
            } catch (JMSException e) {
                this.session.connection.onAsyncException(e);
                return null;
            }

            if (browseDone.get() || !session.isRunning()) {
                destroyConsumer();
                return null;
            }

            waitForMessage();
        }
    }

    public synchronized void close() throws JMSException {
        destroyConsumer();
        closed = true;
    }

    /**
     * Gets the queue associated with this queue browser.
     * 
     * @return the queue
     * @throws JMSException if the JMS provider fails to get the queue
     *                 associated with this browser due to some internal error.
     */

    public Queue getQueue() throws JMSException {
        return (Queue)destination;
    }

    public String getMessageSelector() throws JMSException {
        return selector;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    /**
     * Wait on a semaphore for a fixed amount of time for a message to come in.
     */
    protected void waitForMessage() {
        try {
            synchronized (semaphore) {
                semaphore.wait(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void notifyMessageAvailable() {
        synchronized (semaphore) {
            semaphore.notifyAll();
        }
    }

    public String toString() {
        return "ActiveMQQueueBrowser { value=" + consumerId + " }";
    }

}
