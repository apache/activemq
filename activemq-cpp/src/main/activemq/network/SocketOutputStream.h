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

#ifndef ACTIVEMQ_NETWORK_SOCKETOUTPUTSTREAM_H_
#define ACTIVEMQ_NETWORK_SOCKETOUTPUTSTREAM_H_
 
#include <activemq/io/OutputStream.h>
#include <activemq/network/Socket.h>
#include <activemq/concurrent/Mutex.h>

namespace activemq{
namespace network{
      
   /**
    * Output stream for performing write operations
    * on a socket.
    */
   class SocketOutputStream : public io::OutputStream
   {
   private:
   
      // The socket.
      Socket::SocketHandle socket;
      concurrent::Mutex mutex;
      bool debug;
      
   public:
   
      /**
       * Constructor.
       * @param socket the socket handle.
       */
      SocketOutputStream( Socket::SocketHandle socket );
      
      /**
       * Destructor.
       */
      virtual ~SocketOutputStream();
      
      virtual void setDebug( const bool debug ){
        this->debug = debug;
      }
      
      /**
       * Locks the object.
       */
      virtual void lock() throw(exceptions::ActiveMQException){
         mutex.lock();
      }
   
      /**
       * Unlocks the object.
       */
      virtual void unlock() throw(exceptions::ActiveMQException){   
         mutex.unlock();
      }
       
      /**
       * Waits on a signal from this object, which is generated
       * by a call to Notify.  Must have this object locked before
       * calling.
       */
      virtual void wait() throw(exceptions::ActiveMQException){
         mutex.wait();
      }
    
      /**
       * Waits on a signal from this object, which is generated
       * by a call to Notify.  Must have this object locked before
       * calling.  This wait will timeout after the specified time
       * interval.
       * @param time in millisecsonds to wait, or WAIT_INIFINITE
       * @throws ActiveMQException
       */
      virtual void wait(unsigned long millisecs) 
         throw(exceptions::ActiveMQException) {
         
         mutex.wait(millisecs);
      }

      /**
       * Signals a waiter on this object that it can now wake
       * up and continue.  Must have this object locked before
       * calling.
       */
      virtual void notify() throw(exceptions::ActiveMQException){
         mutex.notify();
      }
        
      /**
       * Signals the waiters on this object that it can now wake
       * up and continue.  Must have this object locked before
       * calling.
       */
      virtual void notifyAll() throw(exceptions::ActiveMQException){
         mutex.notifyAll();
      }
       
       /**
       * Writes a single byte to the output stream.
       * @param c the byte.
       * @throws IOException thrown if an error occurs.
       */
      virtual void write( const unsigned char c ) throw (io::IOException);
      
      /**
       * Writes an array of bytes to the output stream.
       * @param buffer The array of bytes to write.
       * @param len The number of bytes from the buffer to be written.
       * @throws IOException thrown if an error occurs.
       */
      virtual void write( const unsigned char* buffer, const int len ) throw (io::IOException);
      
      /**
       * Flush - does nothing.
       */
      virtual void flush() throw (io::IOException){};
      
      /**
       * Close - does nothing.  It is the responsibility of the owner
       * of the socket object to close it.
       */
      virtual void close() throw(cms::CMSException){} 
   };
   
}}

#endif /*ACTIVEMQ_NETWORK_SOCKETOUTPUTSTREAM_H_*/
