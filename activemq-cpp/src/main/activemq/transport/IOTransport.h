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
 
#ifndef ACTIVEMQ_TRANSPORT_IOTRANSPORT_H_
#define ACTIVEMQ_TRANSPORT_IOTRANSPORT_H_

#include <activemq/transport/Transport.h>
#include <activemq/transport/TransportExceptionListener.h>
#include <activemq/transport/CommandListener.h>
#include <activemq/concurrent/Runnable.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/exceptions/ActiveMQException.h>
#include <activemq/transport/Command.h>

namespace activemq{
namespace transport{
  
    /**
     * Implementation of the Transport interface that performs
     * marshalling of commands to IO streams.  This class does not
     * implement the request method, it only handles oneway messages.
     * A thread polls on the input stream for in-coming commands.  When
     * a command is received, the command listener is notified.  The
     * polling thread is not started until the start method is called.
     * The close method will close the associated streams.  Close can
     * be called explicitly by the user, but is also called in the 
     * destructor.  Once this object has been closed, it cannot be
     * restarted.
     */
    class IOTransport
    :
        public Transport,
        public concurrent::Runnable
    {
    private:
        
        /**
         * Listener to incoming commands.
         */
        CommandListener* listener;
        
        /**
         * Reads commands from the input stream.
         */
        CommandReader* reader;
        
        /**
         * Writes commands to the output stream.
         */
        CommandWriter* writer;
        
        /**
         * Listener of exceptions from this transport.
         */
        TransportExceptionListener* exceptionListener;
        
        /**
         * The input stream for incoming commands.
         */
        io::InputStream* inputStream;
        
        /**
         * The output stream for out-going commands.
         */
        io::OutputStream* outputStream;
        
        /**
         * The polling thread.
         */
        concurrent::Thread* thread;
        
        /**
         * Flag marking this transport as closed.
         */
        bool closed;
        
    private:
    
        /**
         * Notify the excpetion listener
         */
        void fire( exceptions::ActiveMQException& ex ){

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
                if( listener != NULL ){
                    listener->onCommand( command );
                }
            }catch( ... ){}
        }
        
    public:
  
        /**
         * Constructor.
         */
        IOTransport();
        
        /**
         * Destructor - calls close().
         */
        virtual ~IOTransport();
        
        /**
         * Sends a one-way command.  Does not wait for any response from the
         * broker.
         * @param command the command to be sent.
         * @throws CommandIOException if an exception occurs during writing of
         * the command.
         * @throws UnsupportedOperationException if this method is not implemented
         * by this transport.
         */
        virtual void oneway( Command* command ) throw(CommandIOException, exceptions::UnsupportedOperationException);
        
        /**
         * Not supported by this class - throws an exception.
         * @throws UnsupportedOperationException.
         */
        virtual Response* request( Command* command ) throw(CommandIOException, exceptions::UnsupportedOperationException){
            throw exceptions::UnsupportedOperationException( __FILE__, __LINE__, "IOTransport::request() - unsupported operation" );
        }
        
        /**
         * Assigns the command listener for non-response commands.
         * @param listener the listener.
         */
        virtual void setCommandListener( CommandListener* listener ){
            this->listener = listener;
        }
        
        /**
         * Sets the command reader.
         * @param reader the object that will be used for reading command objects.
         */
        virtual void setCommandReader( CommandReader* reader ){
            this->reader = reader;
        }
        
        /**
         * Sets the command writer.
         * @param writer the object that will be used for writing command objects.
         */
        virtual void setCommandWriter( CommandWriter* writer ){
            this->writer = writer;
        }
        
        /**
         * Sets the observer of asynchronous exceptions from this transport.
         * @param listener the listener of transport exceptions.
         */
        virtual void setTransportExceptionListener( TransportExceptionListener* listener ){
            this->exceptionListener = listener;
        }
        
        /**
         * Sets the input stream for in-coming commands.
         * @param is The input stream.
         */
        virtual void setInputStream( io::InputStream* is ){
            this->inputStream = is;
        }
        
        /**
         * Sets the output stream for out-going commands.
         * @param os The output stream.
         */
        virtual void setOutputStream( io::OutputStream* os ){
            this->outputStream = os;
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
        virtual void start() throw( cms::CMSException );
        
        /**
         * Stops the polling thread and closes the streams.  This can
         * be called explicitly, but is also called in the destructor. Once
         * this object has been closed, it cannot be restarted.
         * @throws CMSException if errors occur.
         */
        virtual void close() throw( cms::CMSException );
        
        /**
         * Runs the polling thread.
         */
        virtual void run();
        
    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_IOTRANSPORT_H_*/
