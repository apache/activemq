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
#ifndef _ACTIVEMQ_CONCURRENT_TASKLISTENER_H_
#define _ACTIVEMQ_CONCURRENT_TASKLISTENER_H_

#include <activemq/concurrent/Runnable.h>
#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace concurrent{

    class TaskListener
    {
    public:
    
        virtual ~TaskListener() {}
    
        /**
         * Called when a queued task has completed, the task that
         * finished is passed along for user consumption
         * @param Runnable Pointer to the task that finished
         */
        virtual void onTaskComplete(Runnable* task) = 0;
       
        /**
         * Called when a queued task has thrown an exception while
         * being run.  The Callee should assume that this was an 
         * unrecoverable exeption and that this task is now defunct.
         * @param Runnable Pointer to the task
         * @param The ActiveMQException that was thrown.
         */
        virtual void onTaskException( Runnable* task, 
                                      exceptions::ActiveMQException& ex) = 0;
       
    };

}}

#endif /*_ACTIVEMQ_CONCURRENT_TASKLISTENER_H_*/
