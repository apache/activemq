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
 
#include "Thread.h"
#include <errno.h>

#ifdef unix
    #include <errno.h> // EINTR
    extern int errno;
#else
    #include <process.h> // _endthreadex
#endif

#include <activemq/exceptions/ActiveMQException.h>

using namespace activemq;
using namespace activemq::concurrent;

#ifdef unix
static struct ThreadStaticInitializer {
    // Thread Attribute member
    pthread_attr_t threadAttribute;
    // Static Initializer:
    ThreadStaticInitializer() {
        pthread_attr_init (&threadAttribute);
        pthread_attr_setdetachstate (&threadAttribute, PTHREAD_CREATE_JOINABLE);
    }
} threadStaticInitializer;
#endif

////////////////////////////////////////////////////////////////////////////////
Thread::Thread()
{
    task = this;
    started = false;
    joined = false;
}

////////////////////////////////////////////////////////////////////////////////
Thread::Thread( Runnable* task )
{
    this->task = task;
    started = false;
    joined = false;
}

////////////////////////////////////////////////////////////////////////////////
Thread::~Thread()
{
}

////////////////////////////////////////////////////////////////////////////////
void Thread::start() throw ( exceptions::ActiveMQException )
{
    if (this->started) {
        throw exceptions::ActiveMQException( __FILE__, __LINE__,
            "Thread already started");
    }
    
#ifdef unix
    
    pthread_attr_init (&attributes);
    pthread_attr_setdetachstate (&attributes, PTHREAD_CREATE_JOINABLE);
    int err = pthread_create (
        &this->threadHandle,
        &attributes,
        runCallback,
        this);
    if (err != 0) {
        throw exceptions::ActiveMQException( __FILE__, __LINE__,
            "Coud not start thread");
    }
    
#else

    unsigned int threadId = 0;
    this->threadHandle = 
        (HANDLE)_beginthreadex(NULL, 0, runCallback, this, 0, &threadId);
    if (this->threadHandle == NULL) {
        throw exceptions::ActiveMQException( __FILE__, __LINE__,
            "Coud not start thread");
    }
    
#endif

    // Mark the thread as started.
    started = true;
}

////////////////////////////////////////////////////////////////////////////////
void Thread::join() throw( exceptions::ActiveMQException )
{
    if (!this->started) {
        throw exceptions::ActiveMQException( __FILE__, __LINE__,
            "Thread::join() called without having called Thread::start()");
    }
    if (!this->joined) {
        
#ifdef unix
        pthread_join(this->threadHandle, NULL);
#else
        WaitForSingleObject (this->threadHandle, INFINITE);       
#endif

    }
    this->joined = true;
}

////////////////////////////////////////////////////////////////////////////////
void Thread::sleep( int millisecs )
{
#ifdef unix
    struct timespec rec, rem;
    rec.tv_sec = millisecs / 1000;
    rec.tv_nsec = (millisecs % 1000) * 1000000;
    while( nanosleep( &rec, &rem ) == -1 ){
        if( errno != EINTR ){
            break;
        }
    }
    
#else
    Sleep (millisecs);
#endif
}

////////////////////////////////////////////////////////////////////////////////
unsigned long Thread::getId(void)
{
   #ifdef unix
      return (long)(pthread_self());
   #else
      return GetCurrentThreadId();
   #endif
}

////////////////////////////////////////////////////////////////////////////////
#ifdef unix
void*
#else
unsigned int WINAPI
#endif
Thread::runCallback( void* param )
{
    // Get the instance.
    Thread* thread = (Thread*)param;
    
    // Invoke run on the task.
    thread->task->run();

#ifdef unix
    return NULL;
#else
    // Return 0 if no exception was threwn. Otherwise -1.
    _endthreadex(0); // Needed when using threads and CRT in Windows. Otherwise memleak can appear.
    return 0;
#endif
}

