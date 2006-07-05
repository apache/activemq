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

#ifndef ACTIVEMQ_IO_BUFFEREDINPUTSTREAM_H_
#define ACTIVEMQ_IO_BUFFEREDINPUTSTREAM_H_
 
#include <activemq/io/InputStream.h>
#include <assert.h>

namespace activemq{
namespace io{
      
   /**
    * A wrapper around another input stream that performs
    * a buffered read, where it reads more data than it needs
    * in order to reduce the number of io operations on the
    * input stream.
    */
   class BufferedInputStream : public InputStream
   {
   private:
   
      /**
       * The target input stream.
       */
      InputStream* stream;
      
      /**
       * The internal buffer.
       */
      unsigned char* buffer;
      
      /**
       * The buffer size.
       */
      int bufferSize;
      
      /**
       * The current head of the buffer.
       */
      int head;
      
      /**
       * The current tail of the buffer.
       */
      int tail;
      
   public:
   
      /**
       * Constructor
       * @param stream The target input stream.
       */
      BufferedInputStream( InputStream* stream );
      
      /**
       * Constructor
       * @param stream the target input stream
       * @param bufferSize the size for the internal buffer.
       */
      BufferedInputStream( InputStream* stream, const int bufferSize );
      
      /**
       * Destructor.
       */
      virtual ~BufferedInputStream();
      
      /**
       * Locks the object.
       */
      virtual void lock() throw(exceptions::ActiveMQException){
         assert( stream != NULL );
         stream->lock();
      }
   
      /**
       * Unlocks the object.
       */
      virtual void unlock() throw(exceptions::ActiveMQException){   
         assert( stream != NULL );
         stream->unlock();
      }
       
      /**
       * Waits on a signal from this object, which is generated
       * by a call to Notify.  Must have this object locked before
       * calling.
       */
      virtual void wait() throw(exceptions::ActiveMQException){
         assert( stream != NULL );
         stream->wait();
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
         
         assert( stream != NULL );
         stream->wait(millisecs);
      }

      /**
       * Signals a waiter on this object that it can now wake
       * up and continue.  Must have this object locked before
       * calling.
       */
      virtual void notify() throw(exceptions::ActiveMQException){
         assert( stream != NULL );
         stream->notify();
      }
        
      /**
       * Signals the waiters on this object that it can now wake
       * up and continue.  Must have this object locked before
       * calling.
       */
      virtual void notifyAll() throw(exceptions::ActiveMQException){
         assert( stream != NULL );
         stream->notifyAll();
      }
    
      /**
       * Indcates the number of bytes avaialable.
       * @return the sum of the amount of data avalable
       * in the buffer and the data available on the target
       * input stream.
       */
      virtual int available() const{   
         return (tail-head)+stream->available();
      }
            
      /**
       * Reads a single byte from the buffer.
       * @return The next byte.
       * @throws IOException thrown if an error occurs.
       */
      virtual unsigned char read() throw (IOException);
      
      /**
       * Reads an array of bytes from the buffer.
       * @param buffer (out) the target buffer.
       * @param bufferSize the size of the output buffer.
       * @return The number of bytes read.
       * @throws IOException thrown if an error occurs.
       */
      virtual int read( unsigned char* buffer, const int bufferSize ) throw (IOException);
      
      /**
       * Closes the target input stream.
       */
      virtual void close(void) throw(cms::CMSException);
      
   private:
   
      /**
       * Initializes the internal structures.
       */
      void init( InputStream* stream, const int bufferSize );
      
      /**
       * Populates the buffer with as much data as possible
       * from the target input stream.
       */
      void bufferData(void) throw (IOException);

   };
   
}}

#endif /*ACTIVEMQ_IO_BUFFEREDINPUTSTREAM_H_*/
