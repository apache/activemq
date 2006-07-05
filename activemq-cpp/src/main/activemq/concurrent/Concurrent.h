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
 
#ifndef _ACTIVEMQ_CONCURRENT_CONCURRENT_H_
#define _ACTIVEMQ_CONCURRENT_CONCURRENT_H_

#include <activemq/concurrent/Lock.h>

namespace activemq{
namespace concurrent{

/**
 * The synchronized macro defines a mechanism for snycronizing
 * a scetion of code.  The macro must be passed an object that
 * implements the Syncronizable interface.
 * 
 * The macro works by creating a for loop that will loop exactly
 * once, creating a Lock object that is scoped to the loop.  Once
 * the loop conpletes and exits the Lock object goes out of scope
 * releasing the lock on object W.  For added safety the if else
 * is used because not all compiles restrict the lifetime of 
 * loop variables to the loop, they will however restrict them
 * to the scope of the else.
 * 
 * The macro would be used as follows.
 * 
 * <Syncronizable> X;
 * 
 * somefunction()
 * {
 *    syncronized(X)
 *    {
 *       // Do something that needs syncronizing.
 *    }
 * }
 */

#define WAIT_INFINITE  0xFFFFFFFF

#define synchronized(W)                                                     \
      if(false){}                                                           \
      else                                                                  \
      for(activemq::concurrent::Lock lock_W(W);                                                    \
          lock_W.isLocked(); lock_W.unlock())

}}

#endif /*_ACTIVEMQ_CONCURRENT_CONCURRENT_H_*/
