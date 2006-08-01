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

#ifndef ACTIVEMQ_NETWORK_SOCKETINPUTSTREAM_H_
#define ACTIVEMQ_NETWORK_SOCKETINPUTSTREAM_H_
 
#include <activemq/io/InputStream.h>
#include <activemq/network/Socket.h>
#include <activemq/concurrent/Mutex.h>

namespace activemq{
namespace network{
    
    /**
     * Input stream for performing reads on a socket.
     */
	class SocketInputStream : public io::InputStream
	{
	private:
	
		// The socket handle.
		Socket::SocketHandle socket;
		concurrent::Mutex mutex;
        bool debug;
		
	public:
	
		/**
		 * Constructor.
		 * @param socket the socket handle.
		 */
		SocketInputStream( Socket::SocketHandle socket );
		
		/**
		 * Destructor.
		 */
		virtual ~SocketInputStream();
		
        /**
         * Enables socket level output of the recieved data
         * @param debug true to turn on debugging
         */
        virtual void setDebug( const bool debug ){
            this->debug = debug;
        }
        
        /**
         * Locks the object.
         * @throws ActiveMQException
         */
        virtual void lock() throw( exceptions::ActiveMQException ){
            mutex.lock();
        }
   
        /**
         * Unlocks the object.
         * @throws ActiveMQException
         */
        virtual void unlock() throw( exceptions::ActiveMQException ){   
            mutex.unlock();
        }
       
        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.
         * @throws ActiveMQException
         */
        virtual void wait() throw( exceptions::ActiveMQException ){
            mutex.wait();
        }
    
        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.  This wait will timeout after the specified time
         * interval.
         * @param millisecs time in millisecsonds to wait, or WAIT_INIFINITE
         * @throws ActiveMQException
         */
        virtual void wait( unsigned long millisecs ) 
            throw( exceptions::ActiveMQException ) {
         
            mutex.wait( millisecs );
        }

        /**
         * Signals a waiter on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         * @throws ActiveMQException
         */
        virtual void notify() throw( exceptions::ActiveMQException ){
            mutex.notify();
        }
        
        /**
         * Signals the waiters on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         * @throws ActiveMQException
         */
        virtual void notifyAll() throw( exceptions::ActiveMQException ){
            mutex.notifyAll();
        }
	    
	    /**
	     * Polls instantaneously to see if data is available on 
	     * the socket.
	     * @return 1 if data is currently available on the socket, otherwise 0.
	     */
		virtual int available() const;
		
		/**
		 * Reads a single byte from the buffer.
		 * @return The next byte.
		 * @throws IOException thrown if an error occurs.
		 */
		virtual unsigned char read() throw ( io::IOException );
		
		/**
		 * Reads an array of bytes from the buffer.
		 * @param buffer (out) the target buffer.
		 * @param bufferSize the size of the output buffer.
		 * @return The number of bytes read.
		 * @throws IOException thrown if an error occurs.
		 */
		virtual int read( unsigned char* buffer, const int bufferSize ) throw (io::IOException);
		
		/**
		 * Close - does nothing.  It is the responsibility of the owner
		 * of the socket object to close it.
         * @throws CMSException
		 */
		virtual void close() throw( cms::CMSException ){}
        
	};
	
}}

#endif /*ACTIVEMQ_NETWORK_SOCKETINPUTSTREAM_H_*/
