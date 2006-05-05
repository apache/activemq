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

#ifndef ACTIVEMQ_CONCURRENT_SYNCHRONIZABLE_H
#define ACTIVEMQ_CONCURRENT_SYNCHRONIZABLE_H

namespace activemq{
namespace concurrent{
    
  /**
   * The interface for all synchronizable objects (that is, objects
   * that can be locked and unlocked).
   * @author Nathan Mittler
   */
  class Synchronizable
  {
  public:        // Abstract Interface

	virtual ~Synchronizable(){}
	
    /**
     * Locks the object.
     * @return  true if the lock was successful, otherwise false.
     */
    virtual bool lock() = 0;

    /**
     * Unlocks the object.
     * @return  true if the unlock was successful, otherwise false.
     */
    virtual bool unlock() = 0;

  }; 

}}

#endif /*ACTIVEMQ_CONCURRENT_SYNCHRONIZABLE_H*/
