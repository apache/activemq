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

#ifndef ACTIVEMQ_CONCURRENT_LOCK_H
#define ACTIVEMQ_CONCURRENT_LOCK_H

#include <activemq/concurrent/Synchronizable.h>

namespace activemq{
namespace concurrent{
    
    /**
     * A wrapper class around a given synchronization mechanism that
     * provides automatic release upon destruction.
     * @author  Nathan Mittler
     */
    class Lock
    {
    private:       // Data

        /**
         * Flag to indicate whether or not this object has locked the
         * sync object.
         */
        bool locked;

        /**
         * The synchronizable object to lock/unlock.
         */
        Synchronizable* syncObject;
    
    public:        // Interface

        /**
         * Constructor - initializes the object member and locks
         * the object if desired.
         * @param   object   The sync object to control
         * @param   intiallyLocked  If true, the object will automatically
         * be locked.
         */
        Lock( Synchronizable* object, const bool intiallyLocked = true )
        {
            try{
                syncObject = object;
                locked = false;
            
                if( intiallyLocked )
                {
                    lock();
                }
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }
    
        /**
         * Destructor - Unlocks the object if it is locked.
         */
        virtual ~Lock()
        {
            try{
                if( locked )
                {
                  syncObject->unlock();
                } 
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )      
        }

        /**
         * Locks the object.
         */
        void lock()
        {
            try{
                syncObject->lock();
                locked = true;
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }

        /**
         * Unlocks the object.
         */
        void unlock()
        {
            try{
                 if(locked)
                 {
                     syncObject->unlock();
                     locked = false;
                 }
            }
            AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }

        /**
         * Indicates whether or not the object is locked.
         * @return  true if the object is locked, otherwise false.
         */
        bool isLocked() const{ return locked; }  
    };

}}

#endif // ACTIVEMQ_CONCURRENT_LOCK_H
