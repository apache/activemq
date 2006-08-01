/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _ACTIVEMQ_CORE_ACTIVEMQTRANSACTION_H_
#define _ACTIVEMQ_CORE_ACTIVEMQTRANSACTION_H_

#include <map>
#include <list>

#include <cms/Message.h>
#include <cms/CMSException.h>

#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/TaskListener.h>
#include <activemq/concurrent/Runnable.h>
#include <activemq/connector/TransactionInfo.h>
#include <activemq/exceptions/InvalidStateException.h>
#include <activemq/exceptions/IllegalArgumentException.h>
#include <activemq/util/Properties.h>
#include <activemq/core/ActiveMQSessionResource.h>

namespace activemq{
namespace core{

    class ActiveMQConnection;
    class ActiveMQSession;
    class ActiveMQMessage;
    class ActiveMQMessageListener;

    /**
     * Transaction Management class, hold messages that are to be redelivered
     * upon a request to rollback.  The Tranasction represents an always
     * running transaction, when it is committed or rolled back it silently
     * creates a new transaction for the next set of messages.  The only
     * way to permanently end this tranaction is to delete it.
     * 
     * Configuration options
     * 
     * transaction.redeliveryDelay
     *   Wait time between the redelivery of each message
     * 
     * transaction.maxRedeliveryCount
     *   Max number of times a message can be redelivered, if the session is 
     *   rolled back more than this many time, the message is dropped.
     */                        
    class ActiveMQTransaction : public concurrent::TaskListener,
                                public connector::TransactionInfo,
                                public ActiveMQSessionResource
    {
    private:
    
        // List type for holding messages
        typedef std::list< ActiveMQMessage* > MessageList;
                
        // Mapping of MessageListener Ids to Lists of Messages that are
        // redelivered on a Rollback
        typedef std::map< ActiveMQMessageListener*, MessageList > RollbackMap;
       
    private:
    
        // Connection this Transaction is associated with
        ActiveMQConnection* connection;
        
        // Session this Transaction is associated with
        ActiveMQSession* session;        
        
        // Transaction Info for the current Transaction
        connector::TransactionInfo* transactionInfo;
        
        // Map of ActiveMQMessageListener to Messages to Rollback
        RollbackMap rollbackMap;
        
        // Lock object to protect the rollback Map
        concurrent::Mutex rollbackLock;
        
        // Max number of redeliveries before we quit
        int maxRedeliveries;
        
        // Wait time between sends of message on a rollback
        int redeliveryDelay;
        
        // Mutex that is signaled when all tasks complete.
        concurrent::Mutex tasksDone;
        
        // Count of Tasks that are outstanding
        int taskCount;

    public:
    
        /**
         * Constructor
         * @param connection - Connection to the Broker
         * @param session - the session that contains this transaction
         * @param properties - configuratoin parameters for this object
         */
    	ActiveMQTransaction( ActiveMQConnection* connection,
                             ActiveMQSession* session,
                             const util::Properties& properties );
    
        virtual ~ActiveMQTransaction(void);
                                  
        /**
         * Adds the Message as a part of the Transaction for the specified
         * ActiveMQConsumer.
         * @param message - Message to Transact
         * @param listener - Listener to redeliver to on Rollback
         */
        virtual void addToTransaction( ActiveMQMessage* message,
                                       ActiveMQMessageListener* listener );
                                      
        /**
         * Removes the ActiveMQMessageListener and all of its transacted 
         * messages from the Transaction, this is usually only done when 
         * a ActiveMQMessageListener is destroyed.
         * @param listener - consumer who is to be removed.
         */
        virtual void removeFromTransaction( ActiveMQMessageListener* listener );
        
        /**
         * Commit the current Transaction
         * @throw CMSException
         */
        virtual void commit(void) throw ( exceptions::ActiveMQException );
        
        /**
         * Rollback the current Transaction
         * @throw CMSException
         */
        virtual void rollback(void) throw ( exceptions::ActiveMQException );
        
        /**
         * Get the Transaction Information object for the current 
         * Transaction, returns NULL if no transaction is running
         * @return TransactionInfo
         */
        virtual connector::TransactionInfo* getTransactionInfo(void) const {
            return transactionInfo;
        }

    public:   // TransactionInfo Interface

        /**
         * Gets the Transction Id
         * @return unsigned int Id
         */
        virtual unsigned int getTransactionId(void) const {
            return transactionInfo->getTransactionId();
        }

        /**
         * Sets the Transction Id
         * @param id - unsigned int Id
         */
        virtual void setTransactionId( const unsigned int id ) {
            transactionInfo->setTransactionId( id );
        }

        /**
         * Gets the Session Info that this transaction is attached too
         * @return SessionnInfo pointer
         */
        virtual const connector::SessionInfo* getSessionInfo(void) const {
            return transactionInfo->getSessionInfo();
        }

        /**
         * Gets the Session Info that this transaction is attached too
         * @param session - SessionnInfo pointer
         */
        virtual void setSessionInfo( const connector::SessionInfo* session ) {
            transactionInfo->setSessionInfo( session );
        }

    protected:   // Task Listener Interface
    
        /**
         * Called when a queued task has completed, the task that
         * finished is passed along for user consumption.  The task is
         * deleted and the count of outstanding tasks is reduced.
         * @param task - Runnable Pointer to the task that finished
         */
        virtual void onTaskComplete( concurrent::Runnable* task );
           
         /**
          * Called when a queued task has thrown an exception while
          * being run.  The Callee should assume that this was an 
          * unrecoverable exeption and that this task is now defunct.
          * Deletes the Task and notifies the connection that the
          * exception has occurred.  Reduce the outstanding task count.
          * @param task - Runnable Pointer to the task
          * @param ex - The ActiveMQException that was thrown.
          */
         virtual void onTaskException( concurrent::Runnable* task, 
                                       exceptions::ActiveMQException& ex );

    public:  // ActiveMQSessionResource
    
        /**
         * Retrieve the Connector resource that is associated with
         * this Session resource.
         * @return pointer to a Connector Resource, can be NULL
         */
        virtual connector::ConnectorResource* getConnectorResource(void) {
            return transactionInfo;
        }

    protected:
    
        /**
         * Clean out all Messages from the Rollback Map, deleting the 
         * messages as it goes.  Destroys the Transaction Info object as 
         * well.
         * @throw ActiveMQException
         */
        virtual void clearTransaction(void);

    private:
    
        // Internal class that is used to redeliver one consumers worth
        // of messages from this transaction.
        class RollbackTask : public concurrent::Runnable
        {
        private:
        
            // Wait time before redelivery in millisecs
            int redeliveryDelay;
            
            // Max number of time to redeliver this message
            int maxRedeliveries;

            // Messages to Redeliver
            MessageList messages;

            // Consumer we are redelivering to
            ActiveMQMessageListener* listener;
            
            // Connection to use for sending message acks
            ActiveMQConnection* connection;
            
            // Session for this Transaction
            ActiveMQSession* session;

        public:

            RollbackTask( ActiveMQMessageListener* listener,
                          ActiveMQConnection* connection,
                          ActiveMQSession* session,
                          MessageList& messages,
                          int maxRedeliveries,
                          int redeliveryDelay ){
                            
                // Store State Data.
                this->messages        = messages;
                this->listener        = listener;
                this->redeliveryDelay = redeliveryDelay;
                this->maxRedeliveries = maxRedeliveries;
                this->session         = session;
                this->connection      = connection;
            }

            // Dispatches the Messages to the Consumer.
            virtual void run(void);

        };

    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQTRANSACTION_H_*/
