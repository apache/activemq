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

#ifndef _INTEGRATION_COMMON_ABSTRACTTESTER_H_
#define _INTEGRATION_COMMON_ABSTRACTTESTER_H_

#include "Tester.h"

#include <activemq/concurrent/Mutex.h>

#include <cms/ConnectionFactory.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/MessageProducer.h>

namespace integration{
namespace common{

    class AbstractTester : public Tester
    {
    public:
    
    	AbstractTester( cms::Session::AcknowledgeMode ackMode = 
                            cms::Session::AUTO_ACKNOWLEDGE );
    	virtual ~AbstractTester();
    
        virtual void doSleep(void);

        virtual unsigned int produceTextMessages( 
            cms::MessageProducer& producer,
            unsigned int count );
        virtual unsigned int produceBytesMessages( 
            cms::MessageProducer& producer,
            unsigned int count );

        virtual void waitForMessages( unsigned int count );

        virtual void onException( const cms::CMSException& error );
        virtual void onMessage( const cms::Message* message );

    public:
        
        cms::ConnectionFactory* connectionFactory;
        cms::Connection* connection;
        cms::Session* session;

        unsigned int numReceived;
        activemq::concurrent::Mutex mutex;

    };

}}

#endif /*_INTEGRATION_COMMON_ABSTRACTTESTER_H_*/
