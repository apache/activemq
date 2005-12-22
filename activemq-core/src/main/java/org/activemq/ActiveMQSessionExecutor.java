/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.activemq;

import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;

import org.activemq.command.ConsumerId;
import org.activemq.command.MessageDispatch;
import org.activemq.thread.Task;
import org.activemq.thread.TaskRunner;
import org.activemq.util.JMSExceptionSupport;

/**
 * A utility class used by the Session for dispatching messages asynchronously to consumers
 *
 * @version $Revision$
 * @see javax.jms.Session
 */
public class ActiveMQSessionExecutor implements Task {
    
    private ActiveMQSession session;
    private MessageDispatchChannel messageQueue = new MessageDispatchChannel();
    private boolean dispatchedBySessionPool;
    private TaskRunner taskRunner;

    ActiveMQSessionExecutor(ActiveMQSession session) {
        this.session = session;
    }

    void setDispatchedBySessionPool(boolean value) {
        dispatchedBySessionPool = value;
        wakeup();
    }
    

    void execute(MessageDispatch message) throws InterruptedException {
        if (!session.isAsyncDispatch() && !dispatchedBySessionPool){
            dispatch(message);
        }else {
            messageQueue.enqueue(message);
            wakeup();
        }
    }

    private void wakeup() {
        if( !dispatchedBySessionPool && !messageQueue.isClosed() && messageQueue.isRunning() && !messageQueue.isEmpty() ) {
            try {
                taskRunner.wakeup();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void executeFirst(MessageDispatch message) {
        messageQueue.enqueueFirst(message);
        wakeup();
    }

    boolean hasUncomsumedMessages() {
        return !messageQueue.isEmpty();
    }

    /**
     * implementation of Runnable
     */
    public void run() {
    }
    
    void dispatch(MessageDispatch message){
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
            taskRunner = ActiveMQConnection.SESSION_TASK_RUNNER.createTaskRunner(this);
            wakeup();
        }
    }

    void stop() throws JMSException {
        try {
            if( messageQueue.isRunning() ) {
                messageQueue.stop();
                taskRunner.shutdown();
            }
        } catch (InterruptedException e) {
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
        MessageDispatch message = messageQueue.dequeueNoWait();
        if( message==null ) {
            return false;
        } else {
            dispatch(message);
            return true;
        }
    }

	List getUnconsumedMessages() {
		return messageQueue.removeAll();
	}
    
}