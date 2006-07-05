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
#ifndef ActiveMQ_TransactionContext_hpp_
#define ActiveMQ_TransactionContext_hpp_

#include <list>
#include "activemq/ISynchronization.hpp"
#include "activemq/Connection.hpp"
#include "activemq/TransactionType.hpp"
#include "activemq/command/TransactionId.hpp"
#include "activemq/command/TransactionInfo.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    using namespace ifr;
    using namespace apache::activemq::command;
    class Session;

/*
 * 
 */
class TransactionContext
{
private:
     p<TransactionId>            transactionId ;
     p<Session>                  session ;
     list< p<ISynchronization> > synchronizations ;

public:
    TransactionContext(p<Session> session) ;
    virtual ~TransactionContext() ;

    virtual p<TransactionId> getTransactionId() ;
    virtual void addSynchronization(p<ISynchronization> synchronization) ;
    virtual void begin() ;
    virtual void commit() ;
    virtual void rollback() ;
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_TransactionContext_hpp_*/
