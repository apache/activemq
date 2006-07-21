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
 
#ifndef ACTIVEMQ_TRANSPORT_FUTURERESPONSE_H_
#define ACTIVEMQ_TRANSPORT_FUTURERESPONSE_H_

#include <activemq/concurrent/Mutex.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/transport/Response.h>

#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace transport{
    
    /**
     * A container that holds a response object.  Since this
     * object is Synchronizable, callers can wait on this object
     * and when a response comes in, notify can be called to
     * inform those waiting that the response is now available.
     */
    class FutureResponse : public concurrent::Synchronizable{
    private:
    
        Response* response;
        concurrent::Mutex mutex;
        
    public:
    
        FutureResponse(){
            response = NULL;
        }
        
        virtual ~FutureResponse(){}
        
        /**
         * Locks the object.
         */
        virtual void lock() throw( exceptions::ActiveMQException ){
            mutex.lock();
        }

        /**
         * Unlocks the object.
         */
        virtual void unlock() throw( exceptions::ActiveMQException ){
            mutex.unlock();
        }
    
        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.
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
        virtual void wait( unsigned long millisecs ) 
            throw( exceptions::ActiveMQException )
        {
            mutex.wait( millisecs );
        }

        /**
         * Signals a waiter on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         */
        virtual void notify() throw( exceptions::ActiveMQException ){
            mutex.notify();
        }
    
        /**
         * Signals the waiters on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         */
        virtual void notifyAll() throw( exceptions::ActiveMQException ){ 
            mutex.notifyAll(); 
        }
        
        /**
         * Getters for the response property.
         */
        virtual const Response* getResponse() const{
            return response;
        }        
        virtual Response* getResponse(){
            return response;
        }
        
        /**
         * Setter for the response property.
         */
        virtual void setResponse( Response* response ){
            this->response = response;
        }
    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_FUTURERESPONSE_H_*/
