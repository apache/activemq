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
#ifndef SIMPLE_MUTEX_HPP
#define SIMPLE_MUTEX_HPP

#if (defined(__unix__) || defined(unix) || defined(MACOSX)) && !defined(USG)
	#ifndef unix
		#define unix
	#endif
	#include <pthread.h>
#endif
#if defined(WIN32) || defined(__CYGWIN__)
#if ( !defined(_WIN32_WINNT) || _WIN32_WINNT < 0x0400)
#pragma message "Unsupported platform, Windows NT 4.0 or later required"
#endif
#include <windows.h>
#endif
#include <assert.h>

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace util
      {

/*
 *
 */
class SimpleMutex
{
private:
#ifdef unix
    pthread_mutex_t mutex ;
#else
    CRITICAL_SECTION  mutex ;
#endif

public:
    SimpleMutex() ;
    virtual ~SimpleMutex() ;

    bool trylock() ;
    void lock() ;
    void unlock() ;
} ;

// Optimize all methods via inline code

inline SimpleMutex::SimpleMutex()
{
#ifdef unix
    pthread_mutexattr_t attr ;
    pthread_mutexattr_init(&attr) ;
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE) ;
    pthread_mutex_init(&mutex, &attr) ;
    pthread_mutexattr_destroy(&attr) ;
#else
    InitializeCriticalSection(&mutex) ;
#endif
}

inline SimpleMutex::~SimpleMutex()
{
#ifdef unix
    pthread_mutex_destroy(&mutex) ;
#else
    DeleteCriticalSection(&mutex) ;
#endif
}

inline bool SimpleMutex::trylock()
{
#ifdef unix
    int try_l = pthread_mutex_trylock(&mutex) ;
    if (try_l == 0)
        return true;
    else
        return false ;
#else
    return (TryEnterCriticalSection(&mutex) != 0) ;
#endif
}

inline void SimpleMutex::lock()
{
#ifdef unix
    pthread_mutex_lock(&mutex) ;
#else
    EnterCriticalSection(&mutex) ;
#endif
}

inline void SimpleMutex::unlock()
{
#ifdef unix
    pthread_mutex_unlock(&mutex) ;
#else
    LeaveCriticalSection(&mutex) ;
#endif
}

/* namespace */
      }
    }
  }
}

#endif /*SIMPLE_MUTEX_HPP*/
