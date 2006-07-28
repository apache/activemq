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

#ifndef _ACTIVEMQ_IO_BYTEARRAYINPUTSTREAM_H_
#define _ACTIVEMQ_IO_BYTEARRAYINPUTSTREAM_H_

#include <activemq/io/InputStream.h>
#include <activemq/concurrent/Mutex.h>
#include <vector>
#include <algorithm>

namespace activemq{
namespace io{

    class ByteArrayInputStream : public InputStream
    {
    private:
      
        /** 
         * The Array of Bytes to read from.
         */
        std::vector<unsigned char> buffer;
      
        /**
         * iterator to current position in buffer.
         */
        std::vector<unsigned char>::const_iterator pos;

        /**
         * Synchronization object.
         */
        concurrent::Mutex mutex;
      
    public:
   
        /**
         * Default Constructor
         */
        ByteArrayInputStream(void);
      
        /**
         * Constructor
         * @param initial byte array to use to read from
         * @param the size of the buffer
         */
        ByteArrayInputStream( const unsigned char* buffer,
                              int bufferSize );

        virtual ~ByteArrayInputStream(void);

        /** 
         * Sets the data that this reader uses, replaces any existing
         * data and resets to beginning of the buffer.
         * @param initial byte array to use to read from
         * @param the size of the buffer
         */
        virtual void setByteArray( const unsigned char* buffer,
                                   int bufferSize );

        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.
         * @throws ActiveMQException
         */
        virtual void lock() throw(exceptions::ActiveMQException){
            mutex.lock();
        }
    
        /**
         * Unlocks the object.
         * @throws ActiveMQException
         */
        virtual void unlock() throw(exceptions::ActiveMQException){ 
            mutex.unlock();
        }
        
        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.
         * @throws ActiveMQException
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
        virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
            mutex.wait(millisecs);
        }

        /**
         * Signals a waiter on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         * @throws ActiveMQException
         */
        virtual void notify() throw(exceptions::ActiveMQException){
            mutex.notify();
        }
         
        /**
         * Signals the waiters on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         * @throws ActiveMQException
         */
        virtual void notifyAll() throw(exceptions::ActiveMQException){
            mutex.notifyAll();
        }
       
        /**
         * Indcates the number of bytes avaialable.
         * @return the sum of the amount of data avalable
         * in the buffer and the data available on the target
         * input stream.
         */
        virtual int available() const{   
            return distance(pos, buffer.end());
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
        virtual int read( unsigned char* buffer, const int bufferSize ) 
            throw (IOException);
      
        /**
         * Closes the target input stream.
         * @throws IOException thrown if an error occurs.
         */
        virtual void close() throw(cms::CMSException);

    };

}}

#endif /*_ACTIVEMQ_IO_BYTEARRAYINPUTSTREAM_H_*/
