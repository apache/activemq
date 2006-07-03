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
 
#ifndef ACTIVEMQ_TRANSPORT_TRANSPORTFILTER_H_
#define ACTIVEMQ_TRANSPORT_TRANSPORTFILTER_H_

#include <activemq/transport/Transport.h>
#include <activemq/transport/CommandListener.h>
#include <activemq/transport/Command.h>
#include <activemq/transport/TransportExceptionListener.h>

namespace activemq{
namespace transport{
  
    /**
     * A filter on the transport layer.  Transport
     * filters implement the Transport interface and 
     * optionally delegate calls to another Transport object.
     */
    class TransportFilter 
    : 
        public Transport,
        public CommandListener,
        public TransportExceptionListener
    {
    protected:        
        
        /**
         * The transport that this filter wraps around.
         */
        Transport* next;
        
        /**
         * Flag to indicate whether this object controls
         * the lifetime of the next transport object.
         */
        bool own;
        
        /**
         * Listener to incoming commands.
         */
        CommandListener* commandlistener;
        
        /**
         * Listener of exceptions from this transport.
         */
        TransportExceptionListener* exceptionListener;
        
    protected:
    
        /**
         * Notify the excpetion listener
         */
        void fire( const exceptions::ActiveMQException& ex ){

            if( exceptionListener != NULL ){
                
                try{
                    exceptionListener->onTransportException( this, ex );
                }catch( ... ){}
            }            
        }
        
        /**
         * Notify the command listener.
         */
        void fire( Command* command ){
            
            try{
                if( commandlistener != NULL ){
                    commandlistener->onCommand( command );
                }
            }catch( ... ){}
        }
        
    public:
  
        /**
         * Constructor.
         */
        TransportFilter( Transport* next, const bool own = true ){
            
            this->next = next;
            this->own = own;
            
            commandlistener = NULL;
            exceptionListener = NULL;
                                    
            // Observe the nested transport for events.
            next->setCommandListener( this );
            next->setTransportExceptionListener( this );
        }
        
        /**
         * Destructor - calls close().
         */
        virtual ~TransportFilter(){
            
            if( own ){
                delete next;
                next = NULL;
            }
            
        }
        
        /**
         * Event handler for the receipt of a command.
         * @param command the received command object.
         */
        virtual void onCommand( Command* command ){
            fire( command );
        }
        
        /**
         * Event handler for an exception from a command transport.
         * @param source The source of the exception
         * @param ex The exception.
         */
        virtual void onTransportException( Transport* source, const exceptions::ActiveMQException& ex ){
            fire( ex );
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
        virtual void oneway( Command* command ) throw(CommandIOException, exceptions::UnsupportedOperationException){
            next->oneway( command );
        }
        
        /**
         * Not supported by this class - throws an exception.
         * @throws UnsupportedOperationException.
         */
        virtual Response* request( Command* command ) throw(CommandIOException, exceptions::UnsupportedOperationException){
            return next->request( command );
        }
        
        /**
         * Assigns the command listener for non-response commands.
         * @param listener the listener.
         */
        virtual void setCommandListener( CommandListener* listener ){
            this->commandlistener = listener;
        }
        
        /**
         * Sets the command reader.
         * @param reader the object that will be used for reading command objects.
         */
        virtual void setCommandReader( CommandReader* reader ){
            next->setCommandReader( reader );
        }
        
        /**
         * Sets the command writer.
         * @param writer the object that will be used for writing command objects.
         */
        virtual void setCommandWriter( CommandWriter* writer ){
            next->setCommandWriter( writer );
        }
      
        /**
         * Sets the observer of asynchronous exceptions from this transport.
         * @param listener the listener of transport exceptions.
         */
        virtual void setTransportExceptionListener( TransportExceptionListener* listener ){
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
            
            if( commandlistener == NULL ){
                throw exceptions::ActiveMQException( __FILE__, __LINE__,
                    "commandListener is invalid" );
            }
            
            if( exceptionListener == NULL ){
                throw exceptions::ActiveMQException( __FILE__, __LINE__,
                    "exceptionListener is invalid" );
            }
            
            // Start the delegate transport object.
            next->start();
        }
        
        /**
         * Stops the polling thread and closes the streams.  This can
         * be called explicitly, but is also called in the destructor. Once
         * this object has been closed, it cannot be restarted.
         * @throws CMSException if errors occur.
         */
        virtual void close() throw( cms::CMSException ){
            
            next->close();
        }
        
    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_TRANSPORTFILTER_H_*/
