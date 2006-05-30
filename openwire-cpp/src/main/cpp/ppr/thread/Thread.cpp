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
#include <memory> // auto_ptr
#ifdef unix
#include <errno.h> // EINTR
#else
#include <process.h> // _endthreadex
#endif
#include "ppr/thread/Thread.hpp"

using namespace apache::ppr::thread;

struct InternalThreadParam
{
    p<Thread> threadObject ;
} ;

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

/*
 *
 */
Thread::Thread() : started (false), joined (false)
{
}

/*
 *
 */
Thread::~Thread()
{
}

/*
 *
 */
void Thread::start() throw (runtime_error)
{
    if( this->started )
        throw runtime_error ("Thread already started");

    auto_ptr<InternalThreadParam> threadParam (new InternalThreadParam()) ;
    threadParam->threadObject = smartify(this) ; // smartify() turns this-pointer into a smart pointer.
#ifdef unix
    int err = pthread_create(
                &this->threadHandle,
                &threadStaticInitializer.threadAttribute,
                internal_thread_function,
                threadParam.get() ) ;
    if( err != 0 )
        throw runtime_error ("Coud not start thread") ;
#else
    this->threadHandle = CreateThread(NULL, 0, internal_thread_function, threadParam.get(), 0, NULL) ;
    if (this->threadHandle == NULL)
        throw runtime_error ("Coud not start thread") ;
#endif
    started = true ;
    threadParam.release() ; // (Does not delete threadParam).
    // threadParam is deleted in internal_thread_function() after when run() has returned.
}

p<exception> Thread::join()
{
    if( !this->started )
        throw runtime_error ("Thread::join() called without having called Thread::start()");

    if( !this->joined )
#ifdef unix
        pthread_join(this->threadHandle, NULL) ;
#else
        WaitForSingleObject (this->threadHandle, INFINITE) ;
#endif
    this->joined = true ;
    return threwnException ;
}

void Thread::sleep(int millisecs)
{
#ifdef unix
    struct timespec rec, rem ;
    rec.tv_sec = millisecs / 1000 ;
    rec.tv_nsec = (millisecs % 1000) * 1000000 ;
    while ( nanosleep( &rec, &rem ) == EINTR ) ;
#else
    Sleep (millisecs) ;
#endif
}

#ifdef unix
void* Thread::internal_thread_function (void* param)
#else
unsigned long WINAPI Thread::internal_thread_function (void* param)
#endif
{
    InternalThreadParam* itp = (InternalThreadParam*) param ;
    try
    {
        itp->threadObject->run() ;
    }
    catch (exception* e)
    {
        itp->threadObject->threwnException = newptr (e) ;
    }
    catch (exception& e)
    {
        itp->threadObject->threwnException = new exception (e) ;
    }
    catch (p<exception> e)
    {
        itp->threadObject->threwnException = e ;
    }
    catch (...)
    {
        itp->threadObject->threwnException = new runtime_error ("An unknown exception was thrown") ;
    }
    p<Thread> threadObject = itp->threadObject ;
    delete itp ;
#ifdef unix
    return NULL ;
#else
    // Return 0 if no exception was threwn. Otherwise -1.
    unsigned long retval = (threadObject->threwnException == NULL ? 0 : -1) ;
    _endthreadex(retval) ; // Needed when using threads and CRT in Windows. Otherwise memleak can appear.
    return retval ;
#endif
}

#ifdef UNITTEST

#include <iostream>

class MyThread : public Thread
{
public:
    bool throwException;
protected:
    virtual void run () throw (p<exception>) 
    {
        cout << "Running thread" << endl;
        if (throwException) {
            cout << "Throwing exception" << endl;
            throw p<exception> (new runtime_error ("Oops, something went wrong"));
        } else {
            cout << "Not throwing exception" << endl;
        }
    }
};

void testThread()
{
    p<MyThread> t1 = new MyThread();
    p<MyThread> t2 = new MyThread();
    t1->throwException = false;
    t2->throwException = true;
    t1->start();
    t2->start();
    p<exception> e1 = t1->join();
    p<exception> e2 = t2->join();
    assert (e1 == NULL);
    assert (e2 != NULL);
    assert (strcmp (e2->what(), "Oops, something went wrong") == 0);
}

#endif // UNITTEST
