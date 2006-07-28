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
#ifndef Ppr_Thread_hpp_
#define Ppr_Thread_hpp_

#ifdef unix
#include <pthread.h>
#else
#include <windows.h>
#endif

#include <stdexcept>
#include <assert.h>
#include "ppr/util/ifr/p"


namespace apache
{
  namespace ppr
  {
    namespace thread
    {
      using namespace std;
      using namespace ifr;


/**
*/
struct IThread : Interface
{
    /** Creates a system thread and starts it in a joinable mode.
        @exception runtime_error is thrown if the system could
        not start the thread.
    */
    virtual void start() throw (runtime_error) = 0;

    /** Wait til the thread exits. This is when the run()
        method has returned or has threwn an exception.
        If an exception was threwn in the run() method,
        join() will return the threwn exception. Otherwise
        (if run() returned normally), join() will
        return NULL.
    */
    virtual p<exception> join() = 0;
};

/** 
 *
 */
class Thread : public IThread
{
private:
#ifdef unix
    pthread_t   threadHandle ;
#else
    HANDLE      threadHandle ;
#endif
    /// true if thread is started.
    bool started;
    /// true if thread is joined
    bool joined;
    /// Exception threwn by run().
    p<exception> threwnException;
public:
    /** Construct.
    */
    Thread() ;
    /** Destruct.
    */
    virtual ~Thread() ;

    /** Creates a system thread and starts it in a joinable mode.
        @exception runtime_error is thrown if the system could
        not start the thread.
    */
    virtual void start() throw (runtime_error);

    /** Wait til the thread exits. This is when the run()
        method has returned or has threwn an exception.
        If an exception was threwn in the run() method,
        join() will return the threwn exception. Otherwise
        (if run() returned normally), join() will
        return NULL.
    */
    virtual p<exception> join() ;
public:
    /** Halts execution of the calling thread for a specified no of millisec.
        
        Note that this method is a static method that applies to the
        calling thread and not to the thread object.
    */
    static void sleep(int millisecs) ;
protected:
    /** Derive from Thread and implement this method. This method will be
        called by the thread that is created by start().
        
        If an exception is threwn, the exception instance will be returned
        by join(). If the same exception instance (and not a copy of it)
        should be returned by join(), the implementation must throw
        a pointer to heap where the exception resides like this:
            throw p<exception> (new MyException());
    */
    virtual void run () throw (p<exception>) = 0;

private:
    // Internal thread handling
#ifdef unix
    static void* internal_thread_function (void* param);
#else
    static unsigned long WINAPI internal_thread_function (void* param);
#endif
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_Thread_hpp_*/
