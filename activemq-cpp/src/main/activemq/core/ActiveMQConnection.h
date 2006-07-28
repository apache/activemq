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
#ifndef _ACTIVEMQ_CORE_ACTIVEMQCONNECTION_H_
#define _ACTIVEMQ_CORE_ACTIVEMQCONNECTION_H_

#include <cms/Connection.h>
#include <cms/ExceptionListener.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/core/ActiveMQConnectionData.h>
#include <activemq/core/ActiveMQMessageListener.h>
#include <activemq/core/ActiveMQMessage.h>
#include <activemq/connector/ConsumerMessageListener.h>
#include <activemq/util/Properties.h>

#include <map>
#include <string>

namespace activemq{
namespace core{

    class cms::Session;   
    class ActiveMQConsumer;

    class ActiveMQConnection : 
        public cms::Connection,
        public connector::ConsumerMessageListener
    {
    private:
   
        // the registered exception listener
        cms::ExceptionListener* exceptionListener;
      
        // All the data that is used to connect this Connection
        ActiveMQConnectionData* connectionData;
      
        // Indicates if this Connection is started
        bool started;

        // Indicates that this connection has been closed, it is no longer
        // usable after this becomes true
        bool closed;
      
        // Map of Consumer Ids to ActiveMQMessageListeners
        std::map< unsigned int, ActiveMQMessageListener* > consumers;
      
        // Mutex to lock the Consumers Map
        concurrent::Mutex mutex;
   
    public:

        /**
         * Constructor
         * @param Pointer to an ActiveMQConnectionData object, owned here
         */       
        ActiveMQConnection( ActiveMQConnectionData* connectionData );

        virtual ~ActiveMQConnection(void);
   
    public:   // Connection Interface Methods
   
        /**
         * Creates a new Session to work for this Connection
         */
        virtual cms::Session* createSession(void) throw ( cms::CMSException );
      
        /**
         * Creates a new Session to work for this Connection using the
         * specified acknowledgment mode
         * @param the Acknowledgement Mode to use.
         */
        virtual cms::Session* createSession( cms::Session::AcknowledgeMode ackMode ) 
            throw ( cms::CMSException );
         
        /**
         * Get the Client Id for this session
         * @return string version of Client Id
         */
        virtual std::string getClientId(void) const;
      
        /**
         * Retrieves the Connection Data object for this object.
         * @return pointer to a connection data object.
         */
        virtual ActiveMQConnectionData* getConnectionData(void){
            return connectionData;
        } 
         
        /**
         * Gets the registered Exception Listener for this connection
         * @return pointer to an exception listnener or NULL
         */
        virtual cms::ExceptionListener* getExceptionListener(void) const{
            return exceptionListener; };
      
        /**
         * Sets the registed Exception Listener for this connection
         * @param pointer to and <code>ExceptionListener</code>
         */
        virtual void setExceptionListener( cms::ExceptionListener* listener ){
            exceptionListener = listener; };
         
        /**
         * Close the currently open connection
         * @throws CMSException
         */
        virtual void close(void) throw ( cms::CMSException );
      
        /**
         * Starts or (restarts) a connections delivery of incoming messages
         * @throws CMSException
         */
        virtual void start(void) throw ( cms::CMSException );
      
        /**
         * Stop the flow of incoming messages
         * @throws CMSException
         */
        virtual void stop(void) throw ( cms::CMSException );

    public:   // ActiveMQConnection Methods
   
        /**
         * Adds the ActiveMQMessageListener to the Mapping of Consumer Id's
         * to listeners, all message to that id will be routed to the given
         * listener
         * @param Consumer Id String
         * @param ActiveMQMessageListener Pointer
         */
        virtual void addMessageListener( const unsigned int consumerId,
                                         ActiveMQMessageListener* listener ); 
      
        /**
         * Remove the Listener for the specified Consumer Id
         * @param Consumer Id string
         */
        virtual void removeMessageListener( const unsigned int consumerId );

    private:
   
        /**
         * Notify the excpetion listener
         */
        void fire( exceptions::ActiveMQException& ex )
        {
            if( exceptionListener != NULL )
            {
                try
                {
                    exceptionListener->onException( ex );
                }
                catch(...){}
            }
        }

        /**
         * Called to dispatch a message to a particular consumer.
         * @param consumer the target consumer of the dispatch.
         * @param msg the message to be dispatched.
         */
        virtual void onConsumerMessage( connector::ConsumerInfo* consumer,
                                        core::ActiveMQMessage* message );
   
    };

}}

#endif /*ACTIVEMQCONNECTION_H_*/
