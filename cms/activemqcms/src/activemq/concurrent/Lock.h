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

// Includes.
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
  public:        // Interface

    /**
     * Constructor - initializes the object member and locks
     * the object if desired.
     * @param   object   The sync object to control
     * @param   intiallyLocked  If true, the object will automatically
     *                be locked.
     * @see  SyncObject
     */
	Lock( Synchronizable* object, const bool intiallyLocked = true )
	{
		syncObject = object;
		locked = false;
		
		if( intiallyLocked )
	    {
	      lock();
	    }
	}

    /**
     * Destructor - Unlocks the object if it is locked.
     */
    virtual ~Lock()
    {
	    if( locked )
	    {
	      syncObject->unlock();
	    }
  	}

    /**
     * Locks the object.
     * @return  true if the lock was successful, otherwise false.
     */
    bool lock()
    {
	    locked = syncObject->lock();
	    return locked;
	}

    /**
     * Unlocks the object.
     * @return  true if the unlock was successful, otherwise false.
     */
    bool unlock()
    {
	    syncObject->unlock();
	    locked = false;
	
	    return locked;
  	}

    /**
     * Indicates whether or not the object is locked.
     * @return  true if the object is locked, otherwise false.
     */
    bool isLocked() const{ return locked; } 

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

  };

}}

#endif // ACTIVEMQ_CONCURRENT_LOCK_H
