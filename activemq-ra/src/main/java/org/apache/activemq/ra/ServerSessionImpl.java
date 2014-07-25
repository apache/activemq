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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ServerSession;
import javax.jms.Session;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;

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


    private int serverSessionId = getNextLogId();
    private final Logger log = LoggerFactory.getLogger(ServerSessionImpl.class.getName() + ":" + serverSessionId);

    private ActiveMQSession session;
    private WorkManager workManager;
    private MessageEndpoint endpoint;
    private MessageProducer messageProducer;
    private final ServerSessionPoolImpl pool;

    private Object runControlMutex = new Object();
    private boolean runningFlag;
    /**
     * True if an error was detected that cause this session to be stale. When a
     * session is stale, it should not be used again for proccessing.
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
        this.session.setMessageListener((MessageListener)endpoint);
        this.session.setDeliveryListener(this);
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

    public MessageProducer getMessageProducer() throws JMSException {
        if (messageProducer == null) {
            messageProducer = getSession().createProducer(null);
        }
        return messageProducer;
    }

    /**
     * @see javax.jms.ServerSession#start()
     */
    public void start() throws JMSException {

        synchronized (runControlMutex) {
            if (runningFlag) {
                log.debug("Start request ignored, already running.");
                return;
            }
            runningFlag = true;
        }

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
        } catch (WorkException e) {
            throw (JMSException)new JMSException("Start failed: " + e).initCause(e);
        }
    }

    /**
     * @see java.lang.Runnable#run()
     */
    public void run() {
        log.debug("Running");
        currentBatchSize = 0;
        while (true) {
            log.debug("run loop start");
            try {
                InboundContextSupport.register(this);
                if ( session.isRunning() ) {
                    session.run();
                } else {
                    log.debug("JMS Session {} with unconsumed {} is no longer running (maybe due to loss of connection?), marking ServerSession as stale", session, session.getUnconsumedMessages().size());
                    stale = true;
                }
            } catch (Throwable e) {
                stale = true;
                if ( log.isDebugEnabled() ) {
                    log.debug("Endpoint {} failed to process message.", session, e);
                } else if ( log.isInfoEnabled() ) {
                    log.info("Endpoint {} failed to process message. Reason: " + e.getMessage(), session);
                }
            } finally {
                InboundContextSupport.unregister(this);
                log.debug("run loop end");
                synchronized (runControlMutex) {
                    // This endpoint may have gone stale due to error
                    if (stale) {
                        runningFlag = false;
                        pool.removeFromPool(this);
                        break;
                    }
                    if (!session.hasUncomsumedMessages()) {
                        runningFlag = false;
                        log.debug("Session has no unconsumed message, returning to pool");
                        pool.returnToPool(this);
                        break;
                    }
                }
            }
        }
        log.debug("Run finished");
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
        return "ServerSessionImpl:" + serverSessionId + "{" + session +"}";
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
