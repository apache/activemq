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
#ifndef _ACTIVEMQ_CONCURRENT_POOLEDTHREADLISTENER_H_
#define _ACTIVEMQ_CONCURRENT_POOLEDTHREADLISTENER_H_

#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace concurrent{

    class PooledThread;

    class PooledThreadListener
    {
    public:

        /**
         * Destructor
         */
        virtual ~PooledThreadListener(void) {}

        /**
         * Called by a pooled thread when it is about to begin
         * executing a new task.
         * @param Pointer to the Pooled Thread that is making this call
         */
        virtual void onTaskStarted(PooledThread* thread) = 0;
       
        /**
         * Called by a pooled thread when it has completed a task
         * and is going back to waiting for another task to run
         * @param Pointer the the Pooled Thread that is making this call.
         */
        virtual void onTaskCompleted(PooledThread* thread) = 0;
      
        /**
         * Called by a pooled thread when it has encountered an exception
         * while running a user task, after receiving this notification
         * the callee should assume that the PooledThread is now no longer
         * running.
         * @param Pointer to the Pooled Thread that is making this call
         * @param The Exception that occured.
         */
        virtual void onTaskException( PooledThread* thread, 
                                      exceptions::ActiveMQException& ex) = 0;
       
    };

}}

#endif /*_ACTIVEMQ_CONCURRENT_POOLEDTHREADLISTENER_H_*/
