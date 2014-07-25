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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.activemq.Service;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.usage.SystemUsage;

/**
 * Adapter to the actual persistence mechanism used with ActiveMQ
 *
 *
 */
public interface PersistenceAdapter extends Service {

    /**
     * Returns a set of all the
     * {@link org.apache.activemq.command.ActiveMQDestination} objects that the
     * persistence store is aware exist.
     *
     * @return active destinations
     */
    Set<ActiveMQDestination> getDestinations();

    /**
     * Factory method to create a new queue message store with the given
     * destination name
     *
     * @param destination
     * @return the message store
     * @throws IOException
     */
    MessageStore createQueueMessageStore(ActiveMQQueue destination) throws IOException;

    /**
     * Factory method to create a new topic message store with the given
     * destination name
     *
     * @param destination
     * @return the topic message store
     * @throws IOException
     */
    TopicMessageStore createTopicMessageStore(ActiveMQTopic destination) throws IOException;

    /**
     * Creates and returns a new Job Scheduler store instance.
     *
     * @return a new JobSchedulerStore instance if this Persistence adapter provides its own.
     *
     * @throws IOException If an error occurs while creating the new JobSchedulerStore.
     * @throws UnsupportedOperationException If this adapter does not provide its own
     *                                       scheduler store implementation.
     */
    JobSchedulerStore createJobSchedulerStore() throws IOException, UnsupportedOperationException;

    /**
     * Cleanup method to remove any state associated with the given destination.
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination
     *            Destination to forget
     */
    void removeQueueMessageStore(ActiveMQQueue destination);

    /**
     * Cleanup method to remove any state associated with the given destination
     * This method does not stop the message store (it might not be cached).
     *
     * @param destination
     *            Destination to forget
     */
    void removeTopicMessageStore(ActiveMQTopic destination);

    /**
     * Factory method to create a new persistent prepared transaction store for
     * XA recovery
     *
     * @return transaction store
     * @throws IOException
     */
    TransactionStore createTransactionStore() throws IOException;

    /**
     * This method starts a transaction on the persistent storage - which is
     * nothing to do with JMS or XA transactions - its purely a mechanism to
     * perform multiple writes to a persistent store in 1 transaction as a
     * performance optimization.
     * <p/>
     * Typically one transaction will require one disk synchronization point and
     * so for real high performance its usually faster to perform many writes
     * within the same transaction to minimize latency caused by disk
     * synchronization. This is especially true when using tools like Berkeley
     * Db or embedded JDBC servers.
     *
     * @param context
     * @throws IOException
     */
    void beginTransaction(ConnectionContext context) throws IOException;

    /**
     * Commit a persistence transaction
     *
     * @param context
     * @throws IOException
     *
     * @see PersistenceAdapter#beginTransaction(ConnectionContext context)
     */
    void commitTransaction(ConnectionContext context) throws IOException;

    /**
     * Rollback a persistence transaction
     *
     * @param context
     * @throws IOException
     *
     * @see PersistenceAdapter#beginTransaction(ConnectionContext context)
     */
    void rollbackTransaction(ConnectionContext context) throws IOException;

    /**
     *
     * @return last broker sequence
     * @throws IOException
     */
    long getLastMessageBrokerSequenceId() throws IOException;

    /**
     * Delete's all the messages in the persistent store.
     *
     * @throws IOException
     */
    void deleteAllMessages() throws IOException;

    /**
     * @param usageManager
     *            The UsageManager that is controlling the broker's memory
     *            usage.
     */
    void setUsageManager(SystemUsage usageManager);

    /**
     * Set the name of the broker using the adapter
     *
     * @param brokerName
     */
    void setBrokerName(String brokerName);

    /**
     * Set the directory where any data files should be created
     *
     * @param dir
     */
    void setDirectory(File dir);

    /**
     * @return the directory used by the persistence adaptor
     */
    File getDirectory();

    /**
     * checkpoint any
     *
     * @param sync
     * @throws IOException
     *
     */
    void checkpoint(boolean sync) throws IOException;

    /**
     * A hint to return the size of the store on disk
     *
     * @return disk space used in bytes of 0 if not implemented
     */
    long size();

    /**
     * return the last stored producer sequenceId for this producer Id used to
     * suppress duplicate sends on failover reconnect at the transport when a
     * reconnect occurs
     *
     * @param id
     *            the producerId to find a sequenceId for
     * @return the last stored sequence id or -1 if no suppression needed
     */
    long getLastProducerSequenceId(ProducerId id) throws IOException;
}
