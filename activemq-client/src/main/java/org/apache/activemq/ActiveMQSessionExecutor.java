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

import java.util.List;
import javax.jms.JMSException;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class used by the Session for dispatching messages asynchronously
 * to consumers
 *
 * @see javax.jms.Session
 */
public class ActiveMQSessionExecutor implements Task {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSessionExecutor.class);

    private final ActiveMQSession session;
    private final MessageDispatchChannel messageQueue;
    private boolean dispatchedBySessionPool;
    private volatile TaskRunner taskRunner;
    private boolean startedOrWarnedThatNotStarted;

    ActiveMQSessionExecutor(ActiveMQSession session) {
        this.session = session;
        if (this.session.connection != null && this.session.connection.isMessagePrioritySupported()) {
           this.messageQueue = new SimplePriorityMessageDispatchChannel();
        }else {
            this.messageQueue = new FifoMessageDispatchChannel();
        }
    }

    void setDispatchedBySessionPool(boolean value) {
        dispatchedBySessionPool = value;
        wakeup();
    }

    void execute(MessageDispatch message) throws InterruptedException {

        if (!startedOrWarnedThatNotStarted) {

            ActiveMQConnection connection = session.connection;
            long aboutUnstartedConnectionTimeout = connection.getWarnAboutUnstartedConnectionTimeout();
            if (connection.isStarted() || aboutUnstartedConnectionTimeout < 0L) {
                startedOrWarnedThatNotStarted = true;
            } else {
                long elapsedTime = System.currentTimeMillis() - connection.getTimeCreated();

                // lets only warn when a significant amount of time has passed
                // just in case its normal operation
                if (elapsedTime > aboutUnstartedConnectionTimeout) {
                    LOG.warn("Received a message on a connection which is not yet started. Have you forgotten to call Connection.start()? Connection: " + connection
                             + " Received: " + message);
                    startedOrWarnedThatNotStarted = true;
                }
            }
        }

        if (!session.isSessionAsyncDispatch() && !dispatchedBySessionPool) {
            dispatch(message);
        } else {
            messageQueue.enqueue(message);
            wakeup();
        }
    }

    public void wakeup() {
        if (!dispatchedBySessionPool) {
            if (session.isSessionAsyncDispatch()) {
                try {
                    TaskRunner taskRunner = this.taskRunner;
                    if (taskRunner == null) {
                        synchronized (this) {
                            if (this.taskRunner == null) {
                                if (!isRunning()) {
                                    // stop has been called
                                    return;
                                }
                                this.taskRunner = session.connection.getSessionTaskRunner().createTaskRunner(this,
                                        "ActiveMQ Session: " + session.getSessionId());
                            }
                            taskRunner = this.taskRunner;
                        }
                    }
                    taskRunner.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                while (iterate()) {
                }
            }
        }
    }

    void executeFirst(MessageDispatch message) {
        messageQueue.enqueueFirst(message);
        wakeup();
    }

    public boolean hasUncomsumedMessages() {
        return !messageQueue.isClosed() && messageQueue.isRunning() && !messageQueue.isEmpty();
    }

    void dispatch(MessageDispatch message) {
        // TODO - we should use a Map for this indexed by consumerId
        for (ActiveMQMessageConsumer consumer : this.session.consumers) {
            ConsumerId consumerId = message.getConsumerId();
            if (consumerId.equals(consumer.getConsumerId())) {
                consumer.dispatch(message);
                break;
            }
        }
    }

    synchronized void start() {
        if (!messageQueue.isRunning()) {
            messageQueue.start();
            if (hasUncomsumedMessages()) {
                wakeup();
            }
        }
    }

    void stop() throws JMSException {
        try {
            if (messageQueue.isRunning()) {
                synchronized(this) {
                    messageQueue.stop();
                    if (this.taskRunner != null) {
                        this.taskRunner.shutdown();
                        this.taskRunner = null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw JMSExceptionSupport.create(e);
        }
    }

    boolean isRunning() {
        return messageQueue.isRunning();
    }

    void close() {
        messageQueue.close();
    }

    void clear() {
        messageQueue.clear();
    }

    MessageDispatch dequeueNoWait() {
        return messageQueue.dequeueNoWait();
    }

    protected void clearMessagesInProgress() {
        messageQueue.clear();
    }

    public boolean isEmpty() {
        return messageQueue.isEmpty();
    }

    public boolean iterate() {

        // Deliver any messages queued on the consumer to their listeners.
        for (ActiveMQMessageConsumer consumer : this.session.consumers) {
            if (consumer.iterate()) {
                return true;
            }
        }

        // No messages left queued on the listeners.. so now dispatch messages
        // queued on the session
        MessageDispatch message = messageQueue.dequeueNoWait();
        if (message == null) {
            return false;
        } else {
            dispatch(message);
            return !messageQueue.isEmpty();
        }
    }

    List<MessageDispatch> getUnconsumedMessages() {
        return messageQueue.removeAll();
    }
    
    void waitForQueueRestart() throws InterruptedException {
        synchronized (messageQueue.getMutex()) {
            while (messageQueue.isRunning() == false) {
                if (messageQueue.isClosed()) {
                    break;
                }
                messageQueue.getMutex().wait();
            }
        }
    }
}
