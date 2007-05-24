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

package org.apache.activemq;

import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A utility class used by the Session for dispatching messages asynchronously to consumers
 *
 * @version $Revision$
 * @see javax.jms.Session
 */
public class ActiveMQSessionExecutor implements Task {
    private static final transient Log log = LogFactory.getLog(ActiveMQSessionExecutor.class);

    private ActiveMQSession session;
    private MessageDispatchChannel messageQueue = new MessageDispatchChannel();
    private boolean dispatchedBySessionPool;
    private TaskRunner taskRunner;
    private boolean startedOrWarnedThatNotStarted;
    private long warnAboutUnstartedConnectionTime = 500L;

    ActiveMQSessionExecutor(ActiveMQSession session) {
        this.session = session;
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
            }
            else {
                long elapsedTime = System.currentTimeMillis() - connection.getTimeCreated();

                // lets only warn when a significant amount of time has passed just in case its normal operation
                if (elapsedTime > aboutUnstartedConnectionTimeout) {
                    log.warn("Received a message on a connection which is not yet started. Have you forgotten to call Connection.start()? Connection: " + connection + " Received: " + message);
                    startedOrWarnedThatNotStarted = true;
                }
            }
        }

        if (!session.isSessionAsyncDispatch() && !dispatchedBySessionPool){
            dispatch(message);
        }else {
            messageQueue.enqueue(message);
            wakeup();
        }
    }

    public void wakeup() {
        if( !dispatchedBySessionPool ) {
            if( session.isSessionAsyncDispatch() ) {
                try {
                	if( taskRunner == null ) {
                		taskRunner = session.connection.getSessionTaskRunner().createTaskRunner(this, "ActiveMQ Session: "+session.getSessionId());
                	}
                    taskRunner.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                while( iterate() )
                    ;
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

    void dispatch(MessageDispatch message){

        // TODO  - we should use a Map for this indexed by consumerId
        
        for (Iterator i = this.session.consumers.iterator(); i.hasNext();) {
            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) i.next();
            ConsumerId consumerId = message.getConsumerId();
            if( consumerId.equals(consumer.getConsumerId()) ) {
                consumer.dispatch(message);
            }
        }
    }
    
    synchronized void start() {
        if( !messageQueue.isRunning() ) {
            messageQueue.start();
            if( hasUncomsumedMessages() )
            	wakeup();
        }
    }

    void stop() throws JMSException {
        try {
            if( messageQueue.isRunning() ) {
                messageQueue.stop();
                if( taskRunner!=null ) {
                    taskRunner.shutdown();
                    taskRunner=null;
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
        return (MessageDispatch) messageQueue.dequeueNoWait();
    }
    
    protected void clearMessagesInProgress(){
        messageQueue.clear();
    }

    public boolean isEmpty() {
        return messageQueue.isEmpty();
    }

    public boolean iterate() {

    	// Deliver any messages queued on the consumer to their listeners.
    	for (Iterator i = this.session.consumers.iterator(); i.hasNext();) {
            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) i.next();
        	if( consumer.iterate() ) {
        		return true;
        	}
        }
    	
    	// No messages left queued on the listeners.. so now dispatch messages queued on the session
        MessageDispatch message = messageQueue.dequeueNoWait();
        if( message==null ) {
            return false;
        } else {
            dispatch(message);
            return !messageQueue.isEmpty();
        }
    }

	List getUnconsumedMessages() {
		return messageQueue.removeAll();
	}
    
}
