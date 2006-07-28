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

#ifndef ACTIVEMQ_CONCURRENT_MUTEX_H
#define ACTIVEMQ_CONCURRENT_MUTEX_H

// Includes.
#include <activemq/concurrent/Synchronizable.h>
#include <pthread.h>

namespace activemq{
namespace concurrent{
    
  /**
   * Creates a pthread_mutex_t object. The object is created
   * such that successive locks from the same thread is allowed
   * and will be successful.
   * @author  Nathan Mittler
   * @see  pthread_mutex_t
   */
  class Mutex : public Synchronizable
  {
  public:

    /**
     * Constructor - creates and initializes the mutex.
     */
    Mutex()
    {
	    // Create an attributes object and initialize it.
	    // Assign the recursive attribute so that the same thread may
	    // lock this mutex repeatedly.
	    pthread_mutexattr_t attributes;
	    pthread_mutexattr_init( &attributes );

       #if defined(__USE_UNIX98) || defined(__APPLE__)
	        pthread_mutexattr_settype( &attributes, PTHREAD_MUTEX_RECURSIVE );
       #endif

	    // Initialize the mutex.
	    pthread_mutex_init( &mutex, &attributes );
	
	    // Destroy the attributes.
	    pthread_mutexattr_destroy( &attributes );
  	}

    /**
     * Destructor - destroys the mutex object.
     */
    virtual ~Mutex()
    {
	    // Unlock the mutex.
	    unlock();
	
	    // Destroy the mutex.
	    pthread_mutex_destroy( &mutex );
  	}

    /**
     * Locks the object.
     * @return  true if the lock was successful, otherwise false.
     */
    virtual bool lock()
    {
    	return pthread_mutex_lock( &mutex ) == 0;
  	}

    /**
     * Unlocks the object.
     * @return  true if the unlock was successful, otherwise false.
     */
    virtual bool unlock()
    {
    	return pthread_mutex_unlock( &mutex ) == 0;
  	}

  private:       // Data

    /**
     * The mutex object.
     */
    pthread_mutex_t mutex;

  };

}}

#endif // ACTIVEMQ_CONCURRENT_MUTEX_H
