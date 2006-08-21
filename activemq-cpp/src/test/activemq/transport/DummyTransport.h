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

#ifndef ACTIVEMQ_TANSPORT_DUMMYTRANSPORT_H_
#define ACTIVEMQ_TANSPORT_DUMMYTRANSPORT_H_

#include <activemq/transport/Transport.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/transport/CommandListener.h>
#include <activemq/transport/TransportExceptionListener.h>
#include <activemq/transport/CommandIOException.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/Thread.h>

namespace activemq{
namespace transport{
    
    class DummyTransport : public Transport{
    
    public:
    
        class ResponseBuilder{
        public:
            virtual ~ResponseBuilder(){}
            
            virtual Response* buildResponse( const Command* cmd ) = 0;
        };

        class InternalCommandListener : 
            public CommandListener,
            public concurrent::Thread
        {
        private:

            DummyTransport* transport;
            ResponseBuilder* responseBuilder;
            concurrent::Mutex mutex;
            Command* command;
            bool done;

        public:

            InternalCommandListener(void) {
                command = NULL;
                transport = NULL;
                responseBuilder = NULL;
                done = false;
                
                this->start();
            }

            virtual ~InternalCommandListener() {
                done = true;
                synchronized( &mutex )
                {
                    mutex.notifyAll();
                }
                this->join();
            }

            void setTransport( DummyTransport* transport ){
                this->transport = transport;
            }

            void setResponseBuilder( ResponseBuilder* responseBuilder ) {
                this->responseBuilder = responseBuilder;
            }

            virtual void onCommand( Command* command )
            {
                synchronized( &mutex )
                {
                    this->command = command;
                    
                    mutex.notifyAll();
                }                
            }

            void run(void)
            {
                try
                {
                    synchronized( &mutex )
                    {
                        while( !done )
                        {
                            mutex.wait();
                            
                            if( command == NULL )
                            {
                                continue;
                            }
                            
                            concurrent::Thread::sleep( 100 );
                            
                            if( responseBuilder != NULL && 
                                transport != NULL )
                            {
                                transport->fireCommand( 
                                    responseBuilder->buildResponse( 
                                        command ) );
                                        
                                command = NULL;
                                
                                return;
                            }
                        }
                    }
                }
                AMQ_CATCHALL_NOTHROW()
            }
        };
        
    private:
    
        ResponseBuilder* responseBuilder;
        CommandListener* commandListener;
        CommandListener* outgoingCommandListener;
        TransportExceptionListener* exceptionListener;
        unsigned int nextCommandId;
        concurrent::Mutex commandIdMutex;
        bool own;
        InternalCommandListener defaultListener;
        
    public:
    
        DummyTransport( ResponseBuilder* responseBuilder , 
                        bool own = false,
                        bool useDefOutgoing = false ){
            
            this->responseBuilder = NULL;
            this->commandListener = NULL;
            this->outgoingCommandListener = NULL;
            this->exceptionListener = NULL;
            this->responseBuilder = responseBuilder;
            this->own = own;
            this->nextCommandId = 0;
            if( useDefOutgoing )
            {
                this->outgoingCommandListener = &defaultListener;
                this->defaultListener.setTransport( this );
                this->defaultListener.setResponseBuilder( responseBuilder );
            }
        }
        
        virtual ~DummyTransport(){
            if( own ){
                delete responseBuilder;
            }
        }
        
        void setResponseBuilder( ResponseBuilder* responseBuilder ){
            this->responseBuilder = responseBuilder;
        }
        
        unsigned int getNextCommandId() throw (exceptions::ActiveMQException){
            
            try{
                synchronized( &commandIdMutex ){
                    return ++nextCommandId;
                }
                
                // Should never get here, but some compilers aren't
                // smart enough to figure out we'll never get here.
                return 0;
            }
            AMQ_CATCHALL_THROW( transport::CommandIOException )
        }
        
        virtual void oneway( Command* command ) 
                throw(CommandIOException, exceptions::UnsupportedOperationException)
        {            
            if( outgoingCommandListener != NULL ){
                
                command->setCommandId( getNextCommandId() );
                command->setResponseRequired( false );
                outgoingCommandListener->onCommand( command );
                return;
            }
        }
        
        virtual Response* request( Command* command ) 
            throw(CommandIOException, 
                  exceptions::UnsupportedOperationException)
        {
            if( responseBuilder != NULL ){
                command->setCommandId( getNextCommandId() );
                command->setResponseRequired( true );
                return responseBuilder->buildResponse( command );
            }
            
            throw CommandIOException( __FILE__, __LINE__,
                "no response builder available" );
        }
        
        virtual void setCommandListener( CommandListener* listener ){
            this->commandListener = listener;
        }
        
        virtual void setOutgoingCommandListener( CommandListener* listener ){
            outgoingCommandListener = listener;
        }
        
        virtual void setCommandReader( CommandReader* reader ){}
        
        virtual void setCommandWriter( CommandWriter* writer ){}
        
        virtual void setTransportExceptionListener( 
            TransportExceptionListener* listener )
        {
            this->exceptionListener = listener;
        }
        
        virtual void fireCommand( Command* cmd ){
            if( commandListener != NULL ){
                commandListener->onCommand( cmd );
            }
        }
        
        virtual void fireException( const exceptions::ActiveMQException& ex ){
            if( exceptionListener != NULL ){
                exceptionListener->onTransportException( this, ex );
            }
        }
        
        virtual void start() throw (cms::CMSException){}
        virtual void close() throw (cms::CMSException){}
    };
    
}}

#endif /*ACTIVEMQ_TANSPORT_DUMMYTRANSPORT_H_*/
