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

#ifndef _ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAM_H_
#define _ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAM_H_

#include <activemq/io/OutputStream.h>
#include <activemq/concurrent/Mutex.h>
#include <vector>

namespace activemq{
namespace io{

    class ByteArrayOutputStream : public OutputStream
    {
    private:
         
        /** 
         * The Array of Bytes to read from.
         */
        std::vector<unsigned char> buffer;

        /**
         * Synchronization object.
         */
        concurrent::Mutex mutex;
      
    public:

        /**
         * Default Constructor
         */
        ByteArrayOutputStream(void) {};

        /**
         * Destructor
         */
   	    virtual ~ByteArrayOutputStream(void) {};
      
        /**
         * Get a snapshot of the data
         * @return pointer to the data
         */
        virtual const unsigned char* getByteArray(void) const {
            return &buffer[0];
        }
      
        /**
         * Get the Size of the Internal Buffer
         * @return size of the internal buffer
         */
        virtual int getByteArraySize(void) const {
            return buffer.size();
        }

        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.
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
         * @param time in millisecsonds to wait, or WAIT_INIFINITE
         * @throws ActiveMQException
         */
        virtual void wait( unsigned long millisecs ) throw( exceptions::ActiveMQException ){
            mutex.wait(millisecs);
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
         * Writes a single byte to the output stream.
         * @param c the byte.
         * @throws IOException thrown if an error occurs.
         */
        virtual void write( const unsigned char c ) 
           throw ( IOException );
      
        /**
         * Writes an array of bytes to the output stream.
         * @param buffer The array of bytes to write.
         * @param len The number of bytes from the buffer to be written.
         * @throws IOException thrown if an error occurs.
         */
        virtual void write( const unsigned char* buffer, const int len ) 
            throw ( IOException );
      
        /**
         * Invokes flush on the target output stream, has no affect.
         * @throws IOException
         */
        virtual void flush( void ) throw ( IOException );
      
        /**
         * Clear current Stream contents
         * @throws IOException
         */
        virtual void clear( void ) throw ( IOException );

        /**
         * Invokes close on the target output stream.
         * @throws CMSException
         */
        void close( void ) throw( cms::CMSException );

   };

}}

#endif /*_ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAM_H_*/
