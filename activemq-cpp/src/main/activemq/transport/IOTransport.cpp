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
 
#include "IOTransport.h"
#include "CommandReader.h"
#include "CommandWriter.h"

#include <activemq/concurrent/Concurrent.h>
#include <activemq/exceptions/UnsupportedOperationException.h>

using namespace activemq;
using namespace activemq::transport;
using namespace activemq::concurrent;

////////////////////////////////////////////////////////////////////////////////
IOTransport::IOTransport(){
    
    listener = NULL;
    reader = NULL;
    writer = NULL;
    exceptionListener = NULL;
    inputStream = NULL;
    outputStream = NULL;
    closed = false;
    thread = NULL;
}

////////////////////////////////////////////////////////////////////////////////
IOTransport::~IOTransport(){
    
    close();
}

////////////////////////////////////////////////////////////////////////////////
void IOTransport::oneway( Command* command ) 
    throw(CommandIOException, exceptions::UnsupportedOperationException)
{
    // Make sure the thread has been started.
    if( thread == NULL ){
        throw CommandIOException( 
            __FILE__, __LINE__, 
            "IOTransport::oneway() - transport is not started" );
    }
    
    // Make sure the command object is valid.
    if( command == NULL ){
        throw CommandIOException( 
            __FILE__, __LINE__, 
            "IOTransport::oneway() - attempting to write NULL command" );
    }
    
    // Make sure we have an output strema to write to.
    if( outputStream == NULL ){
        throw CommandIOException( 
            __FILE__, __LINE__, 
            "IOTransport::oneway() - invalid output stream" );
    }
    
    synchronized( outputStream ){
        // Write the command to the output stream.
        writer->writeCommand( command );
    }
}

////////////////////////////////////////////////////////////////////////////////
void IOTransport::start() throw( cms::CMSException ){
    
    // Can't restart a closed transport.
    if( closed ){
        throw CommandIOException( __FILE__, __LINE__, "IOTransport::start() - transport is already closed - cannot restart" );
    }
    
    // If it's already started, do nothing.
    if( thread != NULL ){
        return;
    }    
    
    // Make sure all variables that we need have been set.
    if( inputStream == NULL || outputStream == NULL || 
        reader == NULL || writer == NULL ){
        throw CommandIOException( 
            __FILE__, __LINE__, 
            "IOTransport::start() - "
            "IO sreams and reader/writer must be set before calling start" );
    }
    
    // Init the Command Reader and Writer with the Streams
    reader->setInputStream( inputStream );
    writer->setOutputStream( outputStream );
    
    // Start the polling thread.
    thread = new Thread( this );
    thread->start();    
}

////////////////////////////////////////////////////////////////////////////////
void IOTransport::close() throw( cms::CMSException ){
    
    try{
        // Mark this transport as closed.
        closed = true;
        
        // Wait for the thread to die.
        if( thread != NULL ){
            thread->join();
            delete thread;
            thread = NULL;
        }
        
        /**
         * Close the input stream.
         */
        if( inputStream != NULL ){
            
            synchronized( inputStream ){
                inputStream->close();
                inputStream = NULL;
            }
        }
        
        /**
         * Close the output stream.
         */
        if( outputStream != NULL ){
            
            synchronized( outputStream ){
                outputStream->close();
                outputStream = NULL;
            }
        }
    }
    AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
    AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void IOTransport::run(){
    
   try{
        
      while( !closed ){
        
         int available = 0;            
         synchronized( inputStream ){
            available = inputStream->available();
         }

         if( available > 0 ){
                
             Command* command = NULL;

             synchronized( inputStream ){
                 // Read the next command from the input stream.
                 command = reader->readCommand();
             }
                                
             // Notify the listener.
             fire( command );
         }
         else{
                
             // Sleep for a short time and try again.
             Thread::sleep( 1 );
         }        
      }
        
   }catch( exceptions::ActiveMQException& ex ){
        
      ex.setMark( __FILE__, __LINE__ );

      if( !closed ) {
         fire( ex );
      }
   }
   catch( ... ){
        
      if( !closed ) {
         exceptions::ActiveMQException ex( 
            __FILE__, __LINE__, 
            "IOTransport::run - caught unknown exception" );

         fire( ex );
      }
   }
}

