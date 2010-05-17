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
package org.apache.activemq.store;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.MemoryUsage;

abstract public class AbstractMessageStore implements MessageStore {
    static final FutureTask<Object> FUTURE;
    protected final ActiveMQDestination destination;

    public AbstractMessageStore(ActiveMQDestination destination) {
        this.destination = destination;
    }
    
    public void dispose(ConnectionContext context) {
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setMemoryUsage(MemoryUsage memoryUsage) {
    }

    public void setBatch(MessageId messageId) throws IOException, Exception {
    }

    /**
     * flag to indicate if the store is empty
     * 
     * @return true if the message count is 0
     * @throws Exception
     */
    public boolean isEmpty() throws Exception {
        return getMessageCount() == 0;
    }

    public Future<Object> asyncAddQueueMessage(final ConnectionContext context, final Message message) throws IOException {
        addMessage(context, message);
        return FUTURE;
    }

       
    public Future<Object> asyncAddTopicMessage(final ConnectionContext context, final Message message) throws IOException {
        addMessage(context, message);
        return FUTURE;
    }

    public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
        removeMessage(context, ack);
    }
    
    static class CallableImplementation implements Callable<Object> {
        public Object call() throws Exception {
            return null;
        }
    }

    static {
       FUTURE = new FutureTask<Object>(new CallableImplementation());
       FUTURE.run();
    }
}
