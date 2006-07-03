/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ActiveMQTransaction.h"

#include <activemq/exceptions/NullPointerException.h>
#include <activemq/core/ActiveMQSession.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/core/ActiveMQConsumer.h>
#include <activemq/core/ActiveMQMessage.h>
#include <activemq/util/Integer.h>

#include <activemq/concurrent/ThreadPool.h>

using namespace std;
using namespace cms;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::util;
using namespace activemq::connector;
using namespace activemq::concurrent;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
ActiveMQTransaction::ActiveMQTransaction( ActiveMQConnection* connection,
                                          ActiveMQSession* session,
                                          const Properties& properties )
{
    try
    {
        if(connection == NULL || session == NULL)
        {
            throw NullPointerException(
                __FILE__, __LINE__,
                "ActiveMQTransaction::ActiveMQTransaction - "
                "Initialized with a NULL connection data");
        }
    
        // Store State Data
        this->connection = connection;
        this->session    = session;
        this->taskCount  = 0;
            
        // convert from property Strings to int.
        redeliveryDelay = Integer::parseInt( 
            properties.getProperty("transaction.redeliveryDelay", "25") );
        maxRedeliveries = Integer::parseInt( 
            properties.getProperty("transaction.maxRedeliveryCount", "5") );

        // Start a new Transaction
        transactionInfo = connection->getConnectionData()->
            getConnector()->startTransaction( session->getSessionInfo() );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQTransaction::~ActiveMQTransaction(void)
{
    try
    {
        // Inform the connector we are rolling back before we close so that
        // the provider knows we didn't complete this transaction
        connection->getConnectionData()->getConnector()->
            rollback(transactionInfo, session->getSessionInfo());

        // Clean up
        clearTransaction();
        
        // Must allow all the tasks to complete before we destruct otherwise
        // the callbacks will cause an exception.
        synchronized(&tasksDone)
        {
            while(taskCount != 0)
            {
                tasksDone.wait(1000);
                
                // TODO - Log Here to get some indication if we are stuck
            }
        }
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::clearTransaction(void)
{
    try
    {
        if(transactionInfo != NULL)
        {
            // Dispose of the ProducerInfo
            connection->getConnectionData()->
                getConnector()->destroyResource(transactionInfo);
        }

        synchronized(&rollbackLock)
        {
            // If there are any messages that are being transacted, then 
            // they die once and for all here.
            RollbackMap::iterator itr = rollbackMap.begin();
            
            for(; itr != rollbackMap.end(); ++itr)
            {
                MessageList::iterator msgItr = itr->second.begin();
                
                for(; msgItr != itr->second.end(); ++msgItr)
                {
                   delete *msgItr;
                }
            }

            rollbackMap.clear();
        }
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::addToTransaction( ActiveMQMessage* message,
                                            ActiveMQMessageListener* listener )
{
    synchronized(&rollbackLock)
    {
        // Store in the Multi Map
        rollbackMap[listener].push_back(message);
    }
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::removeFromTransaction(
    ActiveMQMessageListener* listener )
{
    try
    {
        // Delete all the messages, then remove the consumer's entry from
        // the Rollback Map.
        synchronized(&rollbackLock)
        {
            RollbackMap::iterator rb_itr = rollbackMap.find( listener );
            
            if( rb_itr == rollbackMap.end() )
            {
                return;
            }
            
            MessageList::iterator itr = rb_itr->second.begin();
            
            for(; itr != rollbackMap[listener].end(); ++itr)
            {
               delete *itr;
            }
            
            // Erase the entry from the map
            rollbackMap.erase(listener);
        }
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::commit(void) throw ( exceptions::ActiveMQException )
{
    try
    {    
        if(this->transactionInfo == NULL)
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQTransaction::begin - "
                "Commit called before transaction was started.");
        }
        
        // Commit the current Transaction
        connection->getConnectionData()->getConnector()->
            commit( transactionInfo, session->getSessionInfo() );

        // Clean out the Transaction
        clearTransaction();

        // Start a new Transaction
        transactionInfo = connection->getConnectionData()->
            getConnector()->startTransaction( session->getSessionInfo() );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::rollback(void) throw ( exceptions::ActiveMQException )
{
    try
    {    
        if(this->transactionInfo == NULL)
        {
            throw InvalidStateException(
                __FILE__, __LINE__,
                "ActiveMQTransaction::rollback - "
                "Rollback called before transaction was started.");
        }
        
        // Rollback the Transaction
        connection->getConnectionData()->getConnector()->
            rollback( transactionInfo, session->getSessionInfo() );

        // Dispose of the ProducerInfo
        connection->getConnectionData()->
            getConnector()->destroyResource(transactionInfo);

        // Start a new Transaction
        transactionInfo = connection->getConnectionData()->
            getConnector()->startTransaction( session->getSessionInfo() );

        // Create a task for each consumer and copy its message list out
        // to the Rollback task so we can clear the list for new messages
        // that might come in next.
        //  NOTE - This could be turned into a Thread so that the connection
        //  doesn't have to wait on this method to complete an release its
        //  mutex so it can dispatch new messages.  That would however requre
        //  copying the whole map over to the thread.
        synchronized(&rollbackLock)
        {
            RollbackMap::iterator itr = rollbackMap.begin();
            
            for(; itr != rollbackMap.end(); ++itr)
            {
                ThreadPool::getInstance()->queueTask(make_pair(
                    new RollbackTask( itr->first,
                                      connection,
                                      session,
                                      itr->second,
                                      maxRedeliveries,
                                      redeliveryDelay) , this));

                // Count the tasks started.
                taskCount++;

            }
            
            // Clear the map.  Ownership of the messages is now handed off
            // to the rollback tasks.
            rollbackMap.clear();
        }
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::onTaskComplete( Runnable* task )
{
    try
    {
        // Delete the task
        delete task;
        
        taskCount--;
        
        if(taskCount == 0)
        {
            synchronized(&tasksDone)
            {
                tasksDone.notifyAll();
            }
        }
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}
   
////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::onTaskException( Runnable* task, 
                                           exceptions::ActiveMQException& ex )
{
    try
    {
        // Delegate
        onTaskComplete(task);
        
        // Route the Error
        ExceptionListener* listener = connection->getExceptionListener();
        
        if(listener != NULL)
        {
            listener->onException( ex );
        }
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQTransaction::RollbackTask::run(void)
{
    try
    {        
        MessageList::iterator itr = messages.begin();

        for(; itr != messages.end(); ++itr)
        {
            (*itr)->setRedeliveryCount((*itr)->getRedeliveryCount() + 1);
            
            // Redeliver Messages at some point in the future
            Thread::sleep(redeliveryDelay);
            
            if((*itr)->getRedeliveryCount() >= maxRedeliveries)
            {
                // Poison Ack the Message, we give up processing this one
                connection->getConnectionData()->getConnector()->
                    acknowledge( 
                        session->getSessionInfo(), 
                        dynamic_cast< Message* >(*itr), 
                        Connector::PoisonAck );

                // Won't redeliver this so we kill it here.
                delete *itr;
                
                return;
            }
            
            listener->onActiveMQMessage(*itr);
        }
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}
