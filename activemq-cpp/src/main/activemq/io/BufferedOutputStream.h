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

#ifndef ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAM_H_
#define ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAM_H_
 
#include <activemq/io/OutputStream.h>
#include <assert.h>

namespace activemq{
namespace io{
   
   /**
    * Wrapper around another output stream that buffers
    * output before writing to the target output stream.
    */
   class BufferedOutputStream : public OutputStream
   {
   private:
   
      /**
       * The target output stream.
       */
      OutputStream* stream;
      
      /**
       * The internal buffer.
       */
      unsigned char* buffer;
      
      /**
       * The size of the internal buffer.
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
       * Constructor.
       * @param stream the target output stream.
       */
      BufferedOutputStream( OutputStream* stream );
      
      /**
       * Constructor
       * @param stream the target output stream.
       * @param bufSize the size for the internal buffer.
       */
      BufferedOutputStream( OutputStream* stream, const int bufSize );
      
      /**
       * Destructor
       */
      virtual ~BufferedOutputStream();
      
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
       * Writes a single byte to the output stream.
       * @param c the byte.
       * @throws IOException thrown if an error occurs.
       */
      virtual void write( const unsigned char c ) throw (IOException);
      
      /**
       * Writes an array of bytes to the output stream.
       * @param buffer The array of bytes to write.
       * @param len The number of bytes from the buffer to be written.
       * @throws IOException thrown if an error occurs.
       */
      virtual void write( const unsigned char* buffer, const int len ) throw (IOException);
      
      /**
       * Invokes flush on the target output stream.
       */
      virtual void flush() throw (IOException);
      
      /**
       * Invokes close on the target output stream.
       */
      void close() throw(cms::CMSException);
      
   private:
   
      /**
       * Initializes the internal structures.
       */
      void init( OutputStream* stream, const int bufSize );
      
      /**
       * Writes the contents of the buffer to the output stream.
       */
      void emptyBuffer() throw (IOException);
   };

}}

#endif /*ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAM_H_*/
