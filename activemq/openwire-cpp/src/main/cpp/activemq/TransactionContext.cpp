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
#include "activemq/TransactionContext.hpp"
#include "activemq/Session.hpp"

using namespace apache::activemq;

/*
 * 
 */
TransactionContext::TransactionContext(p<Session> session)
{
    this->session       = session ;
    this->transactionId = NULL ;
}

/*
 * 
 */
TransactionContext::~TransactionContext()
{
    // no-op
}

/*
 * 
 */
p<TransactionId> TransactionContext::getTransactionId()
{
    return transactionId ;
}

/*
 * 
 */
void TransactionContext::addSynchronization(p<ISynchronization> synchronization)
{
    synchronizations.push_back(synchronization) ;
}

/*
 * 
 */
void TransactionContext::begin()
{
    if( transactionId == NULL )
    {
        // Create a new local transaction id
        transactionId = session->getConnection()->createLocalTransactionId() ;

        // Create a new transaction command
        p<TransactionInfo> info = new TransactionInfo() ;
        info->setConnectionId( session->getConnection()->getConnectionId() ) ;
        info->setTransactionId( transactionId ) ;
        info->setType( BeginTx ) ;

        // Send begin command to broker
        session->getConnection()->oneway(info) ;
    }
}

/*
 * 
 */
void TransactionContext::commit()
{
    list< p<ISynchronization> >::const_iterator tempIter ;

    // Iterate through each synchronization and call beforeCommit()
    for( tempIter = synchronizations.begin() ;
         tempIter != synchronizations.end() ;
         tempIter++ )
    {
        (*tempIter)->beforeCommit() ;
    }

    if( transactionId != NULL )
    {
        // Create a new transaction command
        p<TransactionInfo> info = new TransactionInfo() ;
        info->setConnectionId( session->getConnection()->getConnectionId() ) ;
        info->setTransactionId( transactionId ) ;
        info->setType( CommitOnePhaseTx ) ;

        // Reset transaction
        transactionId = NULL ;

        // Send commit command to broker
        session->getConnection()->oneway(info) ;
    }

    // Iterate through each synchronization and call afterCommit()
    for( tempIter = synchronizations.begin() ;
         tempIter != synchronizations.end() ;
         tempIter++ )
    {
        (*tempIter)->afterCommit() ;
    }

    // Clear all syncronizations
    synchronizations.clear() ;
}

/*
 * 
 */
void TransactionContext::rollback()
{
    if( transactionId != NULL )
    {
        // Create a new transaction command
        p<TransactionInfo> info = new TransactionInfo() ;
        info->setConnectionId( session->getConnection()->getConnectionId() ) ;
        info->setTransactionId( transactionId ) ;
        info->setType( RollbackTx ) ;

        // Reset transaction
        transactionId = NULL ;

        // Send rollback command to broker
        session->getConnection()->oneway(info) ;
    }

    list< p<ISynchronization> >::const_iterator tempIter ;

    // Iterate through each synchronization and call afterRollback()
    for( tempIter = synchronizations.begin() ;
         tempIter != synchronizations.end() ;
         tempIter++ )
    {
        (*tempIter)->afterRollback() ;
    }
    // Clear all syncronizations
    synchronizations.clear() ;
}
