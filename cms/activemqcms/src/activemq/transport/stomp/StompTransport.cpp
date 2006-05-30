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

#include "StompTransport.h"
#include "ConnectMessage.h"
#include "DisconnectMessage.h"
#include "ErrorMessage.h"
#include "SubscribeMessage.h"
#include "UnsubscribeMessage.h"
#include "StompTextMessage.h"
#include "StompBytesMessage.h"
#include "StompFrame.h"

#include <activemq/ActiveMQTopic.h>
#include <activemq/concurrent/Lock.h>
#include <activemq/transport/TopicListener.h>

#include <time.h>
#include <stdio.h>

using namespace activemq;
using namespace activemq::transport::stomp;
using namespace activemq::io;
using namespace activemq::concurrent;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
StompTransport::StompTransport( const char* host, 
	const int port,
	const char* userName,
	const char* password )
{
	this->host = host;
	this->port = port;
	started = false;
	readerThread = 0;	
	stompIO = NULL;
	bufferedInputStream = NULL;
	bufferedOutputStream = NULL;
    killThread = false;
    exceptionListener = NULL;
}

////////////////////////////////////////////////////////////////////////////////
StompTransport::~StompTransport()
{	
	// Disconnect from the broker
	close();	
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::start() throw( CMSException ){
	
	try{
		
		if( socket.isConnected() ){
            started = true;
			return;
		}
		
		// Initialize in the stopped state - when the thread starts, it
		// will put us in the started state.
		started = false;
			
		// Create the new connection
		socket.connect( host.c_str(), port );
		socket.setSoReceiveTimeout( 10000 );
        socket.setSoLinger( 0 );
        socket.setKeepAlive( true );
		
		// Create the streams and wire them up.
		bufferedInputStream = new BufferedInputStream( socket.getInputStream(), 100000 );
		bufferedOutputStream = new BufferedOutputStream( socket.getOutputStream(), 100000 );
		stompIO = new StompIO( bufferedInputStream, bufferedOutputStream );
		
		// Send the connect request.
		ConnectMessage msg;	
		msg.setLogin( userName );
		msg.setPassword( password );
		sendMessage( &msg );

        // Sleep for 10 milliseconds for the broker to process the
        // connect message.
        milliSleep( 10 );
     
		// Get the response.
		StompMessage* response = readNextMessage();
		if( response == NULL || response->getMessageType() != StompMessage::MSG_CONNECTED ){	
			
			// Disconnect to clean up any resources.	
			close();			
			throw ActiveMQException( "stomp::StompTransport::connect - error reading connect response from broker" );
		}
		
		// Start the reader thread.
        killThread = false;
		pthread_create( &readerThread, NULL, runCallback, this );
			
		// Wait for the started flag to be set.
		while( !started ){
            milliSleep(10);
		}
        
	}catch( ActiveMQException& ex ){
		
        close();        
        notify( ex );	
        throw ex;	
	}
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::milliSleep( const long millis ){
    
    timespec sleepTime;
    sleepTime.tv_sec = millis/1000;
    sleepTime.tv_nsec = (millis - (sleepTime.tv_sec * 1000)) * 1000000;

    // Loop until the full time has elapsed.
    nanosleep( &sleepTime, &sleepTime );
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::close() throw( CMSException ){
	
	ActiveMQException* ex = NULL;	
	
    // Wait for the reader thread to die.
    if( readerThread != 0 ){
        killThread = true;
        pthread_join( readerThread, NULL );         
        readerThread = 0;   
    }		
	
    // Send the disconnect message.
    if( socket.isConnected() ){
        
        DisconnectMessage msg;  
        sendMessage( &msg );
    }
    
    // Close the socket.
    try
    {
        closeSocket();
    }
    catch( ActiveMQException* x ){ ex = x; }	    
    
	if( ex != NULL ){
		notify( *ex );
        throw *ex;
	}
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::closeSocket(){
    
    // Send the disconnect message and destroy the connection.
    if( socket.isConnected() ){
        
        // We have to close the streams first because they
        // depend on the socket streams.
        if( stompIO != NULL ){
            stompIO->close();
        }
        
        // Now we can close the socket.
        socket.close();
    }
    
    // Destroy the streams.
    if( stompIO != NULL ){
        delete stompIO;     
        stompIO = NULL;
    }
    
    if( bufferedInputStream != NULL ){
        
        delete bufferedInputStream;     
        bufferedInputStream = NULL;
    }
    
    if( bufferedOutputStream != NULL ){
        delete bufferedOutputStream;        
        bufferedOutputStream = NULL;
    }
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::stop() throw( CMSException ){
	
	// Lock this class
	//Lock lock( &mutex );
	
	started = false;	
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::notify( const ActiveMQException& ex ){
	
	try{
		
		if( exceptionListener != NULL ){
			exceptionListener->onException( &ex );
		}
		
	}catch( ... ){
		printf( "StompTransport::notif(ActiveMQException&) - caught exception notifying listener\n" );
	}
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::sendMessage( const cms::Topic* topic, const cms::Message* message ){
    
    const cms::TextMessage* textMsg = dynamic_cast<const cms::TextMessage*>(message);
    if( textMsg != NULL ){
        
        StompTextMessage stompMsg;
        stompMsg.setDestination( createDestinationName( topic ).c_str() );
        stompMsg.setTextNoCopy( textMsg->getText() );
        sendMessage( dynamic_cast<const DestinationMessage*>(&stompMsg) );
        return;
    }
    
    const cms::BytesMessage* bytesMsg = dynamic_cast<const cms::BytesMessage*>(message);
    if( bytesMsg != NULL ){
        
        StompBytesMessage stompMsg;
        stompMsg.setDestination( createDestinationName( topic ).c_str() );
        stompMsg.setDataNoCopy( bytesMsg->getData(), bytesMsg->getNumBytes() );
        sendMessage( dynamic_cast<const DestinationMessage*>(&stompMsg) );
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::sendMessage( const StompMessage* msg ){
	
	
	StompFrame* frame = NULL;
	
	try{
		
		// Adapt the message to a stomp frame object.
		frame = protocolAdapter.adapt( msg );
		
		// If the adaptation failed - throw an exception.
		if( frame == NULL ){
			throw ActiveMQException("Unable to adapt message type to stomp frame");
		}
		
		// Lock this class
		Lock lock( &mutex );
				
        // Write the data to the socket.
        if( stompIO != NULL ){            		  
		  stompIO->writeStompFrame( *frame );
        }
		
	}catch( ActiveMQException& ex ){
		
		// Destroy the frame
		delete frame;
		
		// Notify observers of the exception.
		notify( ex );
		
		// Rethrow the exception.
		throw ex;
	}
}

////////////////////////////////////////////////////////////////////////////////
StompMessage* StompTransport::readNextMessage(){
	
	try{
		
		// Lock access to the network layer.
	    Lock lock( &mutex );				
	    
	    // Create a temporary stomp frame.	
		StompFrame* frame = stompIO->readStompFrame();
		
		// Adapt the stomp frame to a message.
		StompMessage* msg = protocolAdapter.adapt( frame );    				
    	
    	// If the adaptation failed - throw an exception.
    	if( msg == NULL ){
    		throw ActiveMQException( "unable to adapt frame to message type" );
    	}   	
    	
    	// return the message.
    	return msg;
	    
	}catch( ActiveMQException& ex ){
		
		printf( "%s\n", ex.getMessage() );
		
		// Notify observers of the exception.
		notify( ex );
		
		// Rethrow the exception.
		throw ex;
	}
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::addMessageListener( const Topic* topic, 
    MessageListener* listener ){
    
    Lock lock( &mutex );
    
    // Create the destination string.
    std::string destination = createDestinationName( topic );
    
    // Determine whether or not we're already subscribed to this topic
    bool subscribed = destinationPool.hasListeners( destination );

    // Add the listener to the topic.
    destinationPool.addListener( destination, listener );
    
    // If this is the first listener on this destination, subscribe.
    if( !subscribed ){
        subscribe( destination );
    }   
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::removeMessageListener( const Topic* topic, 
    MessageListener* listener ){
    
    Lock lock( &mutex );
    
    // Create the destination string.
    std::string destination = createDestinationName( topic );
    
    // Remove this listener from the topic.
    destinationPool.removeListener( destination, listener );
    
    // If there are no longer any listeners of this destination,
    // unsubscribe.
    if( !destinationPool.hasListeners( destination ) ){
        unsubscribe( destination );
    }
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::subscribe( const std::string& destination ){
    
    // Create and initialize a subscribe message.
    SubscribeMessage msg;   
    msg.setDestination( destination.c_str() );
    msg.setAckMode( Session::AUTO_ACKNOWLEDGE );
    
    // Send the message.
    sendMessage( &msg );
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::unsubscribe( const std::string& destination ){
    
    // Create and initialize an unsubscribe message.
    UnsubscribeMessage msg; 
    msg.setDestination( destination.c_str() );
    
    // Send the message.
    sendMessage( &msg );
}

////////////////////////////////////////////////////////////////////////////////
void StompTransport::run(){
	
	// Set the data flow in the started state.
	started = true;
	
	// Loop until the thread is told to die.
	while( !killThread && socket.isConnected() ){
		
		StompMessage* msg = NULL;
		
		try{
			
			if( stompIO->available() == 0 ){
				
				// No data is available - wait for a short time
				// and try again.
				milliSleep( 10 );
				continue;				
			}
			
			// There is data on the socket - read it.
			msg = readNextMessage();
    		
    		// If we got a message, notify listeners.
			if( msg != NULL && started ){
                
	    		switch( msg->getMessageType() ){
                    case StompMessage::MSG_ERROR:{
                    
                        // This is an error frame - relay the error to the 
                        // ExceptionListener.
                        const ErrorMessage* errorMessage = dynamic_cast<const ErrorMessage*>(msg);
                        string errStr = errorMessage->getErrorTitle();
                        errStr += ". ";
                        errStr += errorMessage->getErrorText();
                        ActiveMQException ex( errStr.c_str() );
                        notify( ex );
                        break;
                    }
                    case StompMessage::MSG_TEXT:
                    case StompMessage::MSG_BYTES:{
            
                        // Notify listeners of the destination message.
                        const DestinationMessage* destMsg = dynamic_cast<const DestinationMessage*>( msg );
                        
                        // Notify observers.
                        destinationPool.notify( destMsg->getDestination(), destMsg->getCMSMessage() );          
                        break;
                    }
                    default:{
                        break;
                    }
                }
			}
				
		}catch( ActiveMQException& ex ){
            
            // Close the socket.
            try{
                closeSocket();
            }catch( ... ){
                printf("run - caught exception closing socket\n" );
            }
            
            // Notify observers of the exception.
            notify( ActiveMQException( (string)("stomp::StompTransport::run - caught exception\n\t") + ex.getMessage() ) );
            
            
		}catch( ... ){                       
            
            // Close the socket.
            try{            
                closeSocket();
            }catch( ... ){
                printf("run - caught exception closing socket\n" );
            }
            
            // Notify observers of the exception.
            notify( ActiveMQException( "stomp::StompTransport::run - unknown error reading message" ) );
		}
		
    	// If a message was allocated in this iteration of the loop,
    	// delete it.
    	if( msg != NULL ){
	    	delete msg;
    	}	    	    
	}
}

////////////////////////////////////////////////////////////////////////////////
void* StompTransport::runCallback( void* instance ){
	
	((StompTransport*)instance)->run();
	return NULL;
}


