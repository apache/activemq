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
package org.apache.activemq.store;

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

import java.io.IOException;
import java.util.Set;

/**
 * Adapter to the actual persistence mechanism used with ActiveMQ
 *
 * @version $Revision: 1.3 $
 */
public interface PersistenceAdapter extends Service {

    /**
     * Returns a set of all the {@link org.apache.activemq.command.ActiveMQDestination}
     * objects that the persistence store is aware exist.
     *
     * @return
     */
    public Set getDestinations();

    /**
     * Factory method to create a new queue message store with the given destination name
     */
    public MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException;

    /**
     * Factory method to create a new topic message store with the given destination name
     */
    public TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException;

    /**
     * Factory method to create a new persistent prepared transaction store for XA recovery
     */
    public TransactionStore createTransactionStore() throws IOException;

    /**
     * This method starts a transaction on the persistent storage - which is nothing to
     * do with JMS or XA transactions - its purely a mechanism to perform multiple writes
     * to a persistent store in 1 transaction as a performance optimization.
     * <p/>
     * Typically one transaction will require one disk synchronization point and so for
     * real high performance its usually faster to perform many writes within the same
     * transaction to minimize latency caused by disk synchronization. This is especially
     * true when using tools like Berkeley Db or embedded JDBC servers.
     */
    public void beginTransaction(ConnectionContext context) throws IOException;


    /**
     * Commit a persistence transaction
     *
     * @see PersistenceAdapter#beginTransaction()
     */
    public void commitTransaction(ConnectionContext context) throws IOException;

    /**
     * Rollback a persistence transaction
     *
     * @see PersistenceAdapter#beginTransaction()
     */
    public void rollbackTransaction(ConnectionContext context) throws IOException;
    
    /**
     * 
     * @return
     * @throws IOException
     */
    public long getLastMessageBrokerSequenceId() throws IOException;
    
    /**
     * Delete's all the messages in the persistent store.
     * 
     * @throws IOException
     */
    public void deleteAllMessages() throws IOException;
    
    public boolean isUseExternalMessageReferences();
    public void setUseExternalMessageReferences(boolean enable);
   
}
