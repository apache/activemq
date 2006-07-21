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
 
#ifndef ACTIVEMQ_TRANSPORT_RESPONSECORRELATOR_H_
#define ACTIVEMQ_TRANSPORT_RESPONSECORRELATOR_H_

#include <activemq/transport/TransportFilter.h>
#include <activemq/transport/FutureResponse.h>
#include <activemq/transport/Command.h>
#include <activemq/transport/ExceptionResponse.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/Concurrent.h>
#include <map>

namespace activemq{
namespace transport{
  
    /**
     * This type of transport filter is responsible for correlating
     * asynchronous responses with requests.  Non-response messages
     * are simply sent directly to the CommandListener.  It owns
     * the transport that it
     */
    class ResponseCorrelator : public TransportFilter 
    {
    private:        

        /**
         * The next command id for sent commands.
         */
        unsigned int nextCommandId;        
        
        /**
         * Map of request ids to future response objects.
         */
        std::map<unsigned int, FutureResponse*> requestMap;
        
        /**
         * Maximum amount of time in milliseconds to wait for a response.
         */
        unsigned long maxResponseWaitTime;
        
        /**
         * Sync object for accessing the next command id variable.
         */
        concurrent::Mutex commandIdMutex;
        
        /**
         * Sync object for accessing the request map.
         */
        concurrent::Mutex mapMutex;
        
        /**
         * Flag to indicate the closed state.
         */
        bool closed;
       
    private:
    
        /**
         * Returns the next available command id.
         */
        unsigned int getNextCommandId() throw ( exceptions::ActiveMQException ){
            
            try{
                synchronized( &commandIdMutex ){
                    return ++nextCommandId;
                }
                
                // Should never get here, but some compilers aren't
                // smart enough to figure out we'll never get here.
                return 0;
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }
         
    public:
  
        /**
         * Constructor.
         */
        ResponseCorrelator( Transport* next, const bool own = true )
        :
            TransportFilter( next, own )
        {            
            nextCommandId = 0;
            
            // Default max response wait time to 3 seconds.
            maxResponseWaitTime = 3000;
            
            // Start in the closed state. 
            closed = true;
        }
        
        /**
         * Destructor - calls close().
         */
        virtual ~ResponseCorrelator(){
            
            // Close the transport and destroy it.
            close();
        
            // Don't do anything with the future responses -
            // they should be cleaned up by each requester.                        
        }
        
        /**
         * Gets the maximum wait time for a response in milliseconds.
         */
        virtual unsigned long getMaxResponseWaitTime() const{
            return maxResponseWaitTime;
        }           
        
        /**
         * Sets the maximum wait time for a response in milliseconds.
         */
        virtual void setMaxResponseWaitTime( const unsigned long milliseconds ){
            maxResponseWaitTime = milliseconds;
        }
        
        /**
         * Sends a one-way command.  Does not wait for any response from the
         * broker.
         * @param command the command to be sent.
         * @throws CommandIOException if an exception occurs during writing of
         * the command.
         * @throws UnsupportedOperationException if this method is not implemented
         * by this transport.
         */
        virtual void oneway( Command* command ) 
            throw( CommandIOException, exceptions::UnsupportedOperationException )
        {
            
            try{
                command->setCommandId( getNextCommandId() );
                command->setResponseRequired( false );
                
                if( closed || next == NULL ){
                    throw CommandIOException( __FILE__, __LINE__,
                        "transport already closed" );
                }
                
                next->oneway( command );
            }
            AMQ_CATCH_RETHROW( exceptions::UnsupportedOperationException )
            AMQ_CATCH_RETHROW( CommandIOException )
            AMQ_CATCH_EXCEPTION_CONVERT( exceptions::ActiveMQException, CommandIOException )
            AMQ_CATCHALL_THROW( CommandIOException )
        }
        
        /**
         * Sends the given request to the server and waits for the response.
         * @param command The request to send.
         * @return the response from the server.  This may be of type ExceptionResponse
         * in the case of a distributed error that occurs at the broker.
         * @throws CommandIOException if an error occurs with the request.
         */
        virtual Response* request( Command* command ) 
            throw( CommandIOException, exceptions::UnsupportedOperationException )
        {
            
            try{
                command->setCommandId( getNextCommandId() );
                command->setResponseRequired( true );
                
                // Add a future response object to the map indexed by this
                // command id.
                FutureResponse* futureResponse = 
                   new FutureResponse();
                  
                synchronized( &mapMutex ){
                    requestMap[command->getCommandId()] = futureResponse;
                }                                
                
                // Wait to be notified of the response via the futureResponse
                // object.
                Response* response = NULL;
                synchronized( futureResponse ){
                    
                    // Send the request.
                    next->oneway( command );
                    
                    // Wait for the response to come in.
                    futureResponse->wait( maxResponseWaitTime );                                       
                    
                    // Get the response.
                    response = futureResponse->getResponse();                    
                }                              
                                
                // Perform cleanup on the map.
                synchronized( &mapMutex ){
                    
                    // We've done our waiting - get this thing out
                    // of the map.
                    requestMap.erase( command->getCommandId() );
                    
                    // Destroy the futureResponse.  It is safe to
                    // do this now because the other thread only
                    // accesses the futureResponse within a lock on
                    // the map.
                    delete futureResponse;
                    futureResponse = NULL;
                }
                    
                if( response == NULL ){                                        
                    
                    throw CommandIOException( __FILE__, __LINE__,
                        "response from futureResponse was invalid" );
                }
                
                return response;                
            }
            AMQ_CATCH_RETHROW( exceptions::UnsupportedOperationException )
            AMQ_CATCH_RETHROW( CommandIOException )
            AMQ_CATCH_EXCEPTION_CONVERT( exceptions::ActiveMQException, CommandIOException )
            AMQ_CATCHALL_THROW( CommandIOException )
        }
        
        /**
         * This is called in the context of the nested transport's
         * reading thread.  In the case of a response object,
         * updates the request map and notifies those waiting on the
         * response.  Non-response messages are just delegated to
         * the command listener.
         * @param command the received from the nested transport.
         */
        virtual void onCommand( Command* command ){
            
            // Let's see if the incoming command is a response.
            Response* response = 
               dynamic_cast<Response*>( command );
               
            if( response == NULL ){
                
                // It's a non-response - just notify the listener.
                fire( command );
                return;
            }
                
            // It is a response - let's correlate ...
            synchronized( &mapMutex ){
                
                // Look the future request up based on the correlation id.
                std::map<unsigned int, FutureResponse*>::iterator iter =
                    requestMap.find( response->getCorrelationId() );
                if( iter == requestMap.end() ){
                    
                    // This is not terrible - just log it.
                    printf("ResponseCorrelator::onCommand() - received unknown response for request: %d\n", 
                        response->getCorrelationId() );
                    return;
                }
                
                // Get the future response (if it's in the map, it's not NULL).
                FutureResponse* futureResponse = iter->second;
                
                // If it's an exception response, notify the exception listener.
                ExceptionResponse* exResp = 
                    dynamic_cast<ExceptionResponse*>( response );
                if( exResp != NULL ){
                    const BrokerError* error = exResp->getException();
                    fire( *error );
                }
                
                synchronized( futureResponse ){
                    
                    // Set the response property in the future response.
                    futureResponse->setResponse( response );
                    
                    // Notify all waiting for this response.
                    futureResponse->notifyAll();
                }
            }
        }
        
        /**
         * Assigns the command listener for non-response commands.
         * @param listener the listener.
         */
        virtual void setCommandListener( CommandListener* listener ){
            this->commandlistener = listener;
        }
        
        /**
         * Sets the observer of asynchronous exceptions from this transport.
         * @param listener the listener of transport exceptions.
         */
        virtual void setTransportExceptionListener( 
            TransportExceptionListener* listener )
        {
            this->exceptionListener = listener;
        }
        
        /**
         * Starts this transport object and creates the thread for
         * polling on the input stream for commands.  If this object
         * has been closed, throws an exception.  Before calling start,
         * the caller must set the IO streams and the reader and writer
         * objects.
         * @throws CMSException if an error occurs or if this transport
         * has already been closed.
         */
        virtual void start() throw( cms::CMSException ){
            
            /**
             * We're already started.
             */
            if( !closed ){
                return;
            }
            
            if( commandlistener == NULL ){
                throw exceptions::ActiveMQException( __FILE__, __LINE__,
                    "commandListener is invalid" );
            }
            
            if( exceptionListener == NULL ){
                throw exceptions::ActiveMQException( __FILE__, __LINE__,
                    "exceptionListener is invalid" );
            }
            
            if( next == NULL ){
                throw exceptions::ActiveMQException( __FILE__, __LINE__,
                    "next transport is NULL" );
            }
            
            // Start the delegate transport object.
            next->start();
            
            // Mark it as open.
            closed = false;
        }
        
        /**
         * Stops the polling thread and closes the streams.  This can
         * be called explicitly, but is also called in the destructor. Once
         * this object has been closed, it cannot be restarted.
         * @throws CMSException if errors occur.
         */
        virtual void close() throw( cms::CMSException ){
            
            if( !closed && next != NULL ){
                next->close();
            }
            
            closed = true;
        }
        
    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_RESPONSECORRELATOR_H_*/
