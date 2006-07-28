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
#include "ActiveMQProducer.h"

#include <activemq/core/ActiveMQSession.h>
#include <activemq/exceptions/NullPointerException.h>

using namespace std;
using namespace activemq;
using namespace activemq::core;
using namespace activemq::connector;
using namespace activemq::exceptions;

////////////////////////////////////////////////////////////////////////////////
ActiveMQProducer::ActiveMQProducer( connector::ProducerInfo* producerInfo,
                                    ActiveMQSession* session )
{
    if( session == NULL || producerInfo == NULL )
    {
        throw NullPointerException(
            __FILE__, __LINE__,
            "ActiveMQProducer::ActiveMQProducer - Init with NULL Session" );
    }
    
    // Init Producer Data
    this->session      = session;
    this->producerInfo = producerInfo;

    // Default the Delivery options
    deliveryMode      = cms::DeliveryMode::PERSISTANT;
    disableMsgId      = false;
    disableTimestamps = false;
    priority          = 4;
    timeToLive        = 0;
}

////////////////////////////////////////////////////////////////////////////////
ActiveMQProducer::~ActiveMQProducer(void)
{
    try
    {
        // Dispose of the ProducerInfo
        session->onDestroySessionResource( this );
    }
    AMQ_CATCH_NOTHROW( ActiveMQException )
    AMQ_CATCHALL_NOTHROW( )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQProducer::send( cms::Message* message ) 
    throw ( cms::CMSException )
{
    try
    {
        send( &producerInfo->getDestination(), message );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ActiveMQProducer::send( const cms::Destination* destination,
                             cms::Message* message) throw ( cms::CMSException )
{
    try
    {
        // configure the message
        message->setCMSDestination( destination );
        message->setCMSDeliveryMode( deliveryMode );
        message->setCMSPriority( priority );
        message->setCMSExpiration( timeToLive );

        session->send( message, this );
    }
    AMQ_CATCH_RETHROW( ActiveMQException )
    AMQ_CATCHALL_THROW( ActiveMQException )
}
