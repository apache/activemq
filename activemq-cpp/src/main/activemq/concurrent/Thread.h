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
#ifndef ACTIVEMQ_CONCURRENT_THREAD_H
#define ACTIVEMQ_CONCURRENT_THREAD_H

#include <activemq/exceptions/ActiveMQException.h>
#include <activemq/concurrent/Runnable.h>
#include <stdexcept>
#include <assert.h>

#if (defined(__unix__) || defined(unix) || defined(MACOSX) || defined(__APPLE__)) && !defined(USG)
   
    #ifndef unix
        #define unix
    #endif

    #include <pthread.h>
#else
    #include <windows.h>
#endif

namespace activemq{
namespace concurrent{
   
    /** 
     * Basic thread class - mimics the Java Thread.  Derived classes may 
     * implement the run method, or this class can be used as is with 
     * a provided Runnable delegate.
     */
    class Thread : public Runnable
    {
    private:
   
        /**
         * The task to be run by this thread, defaults to 
         * this thread object.
         */
        Runnable* task;
      
        #ifdef unix
            pthread_attr_t attributes;
            pthread_t threadHandle;
        #else
            HANDLE threadHandle;
        #endif
   
        /**
         * Started state of this thread.
         */
        bool started;
       
        /**
         * Indicates whether the thread has already been
         * joined.
         */
        bool joined;
       
    public:
      
        /**
         * default Constructor
         */
        Thread();
        
        /**
         * Constructor
         * @param task the Runnable that this thread manages
         */
        Thread( Runnable* task );    
         
        virtual ~Thread();
   
        /** 
         * Creates a system thread and starts it in a joinable mode.  
         * Upon creation, the
         * run() method of either this object or the provided Runnable
         * object will be invoked in the context of this thread.
         * @exception runtime_error is thrown if the system could
         * not start the thread.
         */
        virtual void start() throw ( exceptions::ActiveMQException );
   
        /**
         * Wait til the thread exits. This is when the run()
         * method has returned or has thrown an exception.
         */
        virtual void join() throw ( exceptions::ActiveMQException );
       
        /**
         * Default implementation of the run method - does nothing.
         */
        virtual void run(){};
       
    public:
   
        /**
         * Halts execution of the calling thread for a specified no of millisec.
         *   
         * Note that this method is a static method that applies to the
         * calling thread and not to the thread object.
         * @param millisecs time in milliseconds to sleep
         */
        static void sleep( int millisecs );
       
        /**
         * Obtains the Thread Id of the current thread
         * @return Thread Id
         */
        static unsigned long getId(void); 
   
    private:
   
        // Internal thread handling
        #ifdef unix
            static void* runCallback (void* param);
        #else
            static unsigned int WINAPI runCallback (void* param);
        #endif

    };

}}

#endif /*ACTIVEMQ_CONCURRENT_THREAD_H*/
