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
package org.apache.activemq.ra;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.ServerSession;
import jakarta.jms.Session;
import jakarta.resource.spi.endpoint.MessageEndpoint;
import jakarta.resource.spi.work.Work;
import jakarta.resource.spi.work.WorkEvent;
import jakarta.resource.spi.work.WorkException;
import jakarta.resource.spi.work.WorkListener;
import jakarta.resource.spi.work.WorkManager;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.ActiveMQSession.DeliveryListener;
import org.apache.activemq.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ServerSessionImpl implements ServerSession, InboundContext, Work, DeliveryListener {

    public static final Method ON_MESSAGE_METHOD;
    private static int nextLogId;

    static {
        try {
            ON_MESSAGE_METHOD = MessageListener.class.getMethod("onMessage", new Class[] {
                Message.class
            });
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }


    private final Logger log = LoggerFactory.getLogger(ServerSessionImpl.class);

    private ActiveMQSession session;
    private WorkManager workManager;
    private MessageEndpoint endpoint;
    private MessageProducer messageProducer;
    private final ServerSessionPoolImpl pool;

    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * True if an error was detected that cause this session to be stale. When a
     * session is stale, it should not be used again for processing.
     */
    private boolean stale;
    /**
     * Does the TX commit need to be managed by the RA?
     */
    private final boolean useRAManagedTx;
    /**
     * The maximum number of messages to batch
     */
    private final int batchSize;
    /**
     * The current number of messages in the batch
     */
    private int currentBatchSize;

    public ServerSessionImpl(ServerSessionPoolImpl pool, ActiveMQSession session, WorkManager workManager, MessageEndpoint endpoint, boolean useRAManagedTx, int batchSize) throws JMSException {
        this.pool = pool;
        this.session = session;
        this.workManager = workManager;
        this.endpoint = endpoint;
        this.useRAManagedTx = useRAManagedTx;
        this.batchSize = batchSize;
    }

    private static synchronized int getNextLogId() {
        return nextLogId++;
    }

    public Session getSession() throws JMSException {
        return session;
    }

    protected boolean isStale() {
        return stale || !session.isRunning();
    }

    protected boolean isRunning() {
        return running.get();
    }

    public MessageProducer getMessageProducer() throws JMSException {
        if (messageProducer == null) {
            messageProducer = getSession().createProducer(null);
        }
        return messageProducer;
    }

    /**
     * @see jakarta.jms.ServerSession#start()
     */
    public void start() throws JMSException {

        if (!running.compareAndSet(false, true)) {
            log.debug("Start request ignored, already running.");
            return; // already running
        }

        // only start dispatching messages to the listener when we actually start the worker
        this.session.setMessageListener((MessageListener)endpoint);
        this.session.setDeliveryListener(this);

        // We get here because we need to start a async worker.
        log.debug("Starting run.");
        try {
            workManager.scheduleWork(this, WorkManager.INDEFINITE, null, new WorkListener() {
                // The work listener is useful only for debugging...
                public void workAccepted(WorkEvent event) {
                    log.debug("Work accepted: " + event);
                }

                public void workRejected(WorkEvent event) {
                    log.debug("Work rejected: " + event);
                }

                public void workStarted(WorkEvent event) {
                    log.debug("Work started: " + event);
                }

                public void workCompleted(WorkEvent event) {
                    log.debug("Work completed: " + event);
                }

            });
        } catch (final WorkException e) {
            log.warn("Failed to schedule work for session {}, marking not running", this, e);
            running.set(false); // make sure we don't leave the ServerSession in a running state (misleading)
            throw (JMSException)new JMSException("Start failed: " + e).initCause(e);
        }
    }

    /**
     * @see java.lang.Runnable#run()
     */
    public void run() {
        log.debug("{} Running", this);
        currentBatchSize = 0;
        while (true) {
            log.debug("{} run loop", this);
            try {
                InboundContextSupport.register(this);
                if (session.isClosed()) {
                    stale = true;
                } else if (session.isRunning() ) {
                    session.run();
                } else {
                    log.debug("JMS Session {} with unconsumed {} is no longer running (maybe due to loss of connection?), marking ServerSession as stale", session, session.getUnconsumedMessages().size());
                    stale = true;
                }
            } catch (Throwable e) {
                stale = true;
                if ( log.isDebugEnabled() ) {
                    log.debug("Endpoint {} failed to process message.", this, e);
                } else if ( log.isInfoEnabled() ) {
                    log.info("Endpoint {} failed to process message. Reason: {}", this, e.getMessage());
                }
            } finally {
                InboundContextSupport.unregister(this);
                log.debug("run loop end");
                // This endpoint may have gone stale due to error
                if (stale) {
                    log.debug("Session {} stale, removing from pool", this);
                    running.set(false);
                    pool.removeFromPool(this);
                    break;
                }
                if (!session.hasUncomsumedMessages()) {
                    log.debug("Session {} has no unconsumed message, returning to pool", this);
                    running.set(false);
                    pool.returnToPool(this);
                    break;
                } else {
                    log.debug("Session {} has more work to do b/c of unconsumed", this);
                }
            }
        }
        log.debug("{} Run finished", this);
    }

    /**
     * The ActiveMQSession's run method will call back to this method before
     * dispactching a message to the MessageListener.
     */
    public void beforeDelivery(ActiveMQSession session, Message msg) {
        if (currentBatchSize == 0) {
            try {
                endpoint.beforeDelivery(ON_MESSAGE_METHOD);
            } catch (Throwable e) {
                throw new RuntimeException("Endpoint before delivery notification failure", e);
            }
        }
    }

    /**
     * The ActiveMQSession's run method will call back to this method after
     * dispactching a message to the MessageListener.
     */
    public void afterDelivery(ActiveMQSession session, Message msg) {
        if (++currentBatchSize >= batchSize || !session.hasUncomsumedMessages()) {
            currentBatchSize = 0;
            try {
                endpoint.afterDelivery();
            } catch (Throwable e) {
                throw new RuntimeException("Endpoint after delivery notification failure: " + e, e);
            } finally {
                TransactionContext transactionContext = session.getTransactionContext();
                if (transactionContext != null && transactionContext.isInLocalTransaction()) {
                    if (!useRAManagedTx) {
                        // Sanitiy Check: If the local transaction has not been
                        // commited..
                        // Commit it now.
                        log.warn("Local transaction had not been commited. Commiting now.");
                    }
                    try {
                        session.commit();
                    } catch (JMSException e) {
                        log.info("Commit failed:", e);
                    }
                }
            }
        }
    }

    /**
     * @see javax.resource.spi.work.Work#release()
     */
    public void release() {
        log.debug("release called");
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "ServerSessionImpl:{" + session +"}";
    }

    public void close() {
        try {
            endpoint.release();
        } catch (Throwable e) {
            log.debug("Endpoint did not release properly: " + e.getMessage(), e);
        }
        try {
            session.close();
        } catch (Throwable e) {
            log.debug("Session did not close properly: " + e.getMessage(), e);
        }
    }

}
