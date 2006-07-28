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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPTRANSPORT_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPTRANSPORT_H_

#include <activemq/transport/Transport.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/transport/stomp/AggregateProtocolAdapter.h>
#include <activemq/transport/stomp/StompFrame.h>
#include <activemq/transport/stomp/StompIO.h>
#include <activemq/transport/stomp/DestinationPool.h>
#include <activemq/io/BufferedInputStream.h>
#include <activemq/io/BufferedOutputStream.h>
#include <activemq/io/Socket.h>
#include <pthread.h>
#include <vector>

namespace activemq{
namespace transport{
namespace stomp{
	
	// Forward declarations.
	class TransportListener;
	
    /**
     * Implementation of the transport interface
     * for the stomp protocol.
     * @author Nathan Mittler
     */
	class StompTransport : public Transport
	{
	public:
	
		StompTransport( const char* host, 
			const int port,
			const char* userName,
			const char* password );
		virtual ~StompTransport();
        
        /**
         * Disconnects from the broker.
         */
        virtual void close() throw (cms::CMSException);
        
        /**
         * Connects if necessary and starts the flow of messages to observers.
         */
        virtual void start() throw( cms::CMSException );
        
        /**
         * Stops the flow of messages to observers.  Messages
         * will not be saved, so messages arriving after this call
         * will be lost.
         */
        virtual void stop() throw( cms::CMSException );
        
        /**
         * Sends a message to the broker on the given topic.
         * @param topic The topic on which to send the message.
         * @param message The message to send.
         */
        virtual void sendMessage( const cms::Topic* topic, const cms::Message* message );
        
        virtual void addMessageListener( const cms::Topic* topic,
            cms::MessageListener* listener );
        virtual void removeMessageListener( const cms::Topic* topic,
            cms::MessageListener* listener );
            
        /**
         * Sets the listener of transport exceptions.
         */
        virtual void setExceptionListener( cms::ExceptionListener* listener ){            
            exceptionListener = listener;
        }       
		
	private:
    
        void closeSocket();
        
        std::string createDestinationName( const cms::Topic* topic ){
            return ((std::string)"/topic/") + topic->getTopicName();
        }
        
        std::string createTopicName( const std::string& destination ){
            std::string topicName = destination.substr( 7 );
            return topicName;
        }
        
        void subscribe( const std::string& destination );
        void unsubscribe( const std::string& destination );
        void milliSleep( const long millis );
		void sendMessage( const StompMessage* msg );
		void notify( const ActiveMQException& ex );
		
		StompMessage* readNextMessage();
		
		void run();
		
		/**
		 * The run method for the reader thread.
		 */
		static void* runCallback( void* );
		
	private:
	
        /**
         * Pool of STOMP destinations and the subscribers to those
         * destinations.
         */
        DestinationPool destinationPool;
        
		/**
		 * Listener to this transport channel.
		 */
		TransportListener* listener;
		
		/**
		 * The client socket.
		 */
		io::Socket socket;
		
		/**
		 * Indicates whether or not the flow of data to listeners is
		 * started.
		 */
		bool started;
        
        /**
         * Flag to control the alive state of the IO thread.
         */
        bool killThread;
		
		/**
		 * The broker host name.
		 */
		std::string host;
		
		/**
		 * The broker port.
		 */
		int port;
		
		std::string userName;
		std::string password;
		
		/**
		 * Synchronization object.
		 */
		concurrent::Mutex mutex;
		
		/**
		 * The reader thread.
		 */
		pthread_t readerThread;
				
		/**
		 * Protocol adapter for going between messages
		 * and stomp frames.
		 */
		AggregateProtocolAdapter protocolAdapter;		
		
		/**
		 * IO for stomp messages.
		 */
		StompIO* stompIO;
		
		/**
		 * Buffers input from the socket stream.
		 */
		io::BufferedInputStream* bufferedInputStream;
		
		/**
		 * Buffers output to the socket stream.
		 */
		io::BufferedOutputStream* bufferedOutputStream;
        
        /**
         * Listener to exceptions.
         */
        cms::ExceptionListener* exceptionListener;
	};
	
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPTRANSPORT_H_*/
