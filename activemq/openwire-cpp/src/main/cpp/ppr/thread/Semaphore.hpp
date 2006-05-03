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
#ifndef Ppr_Semaphore_hpp_
#define Ppr_Semaphore_hpp_

#if defined MACOSX
#include <errno.h>
#include <time.h>
#include <pthread.h>
#elif defined(__unix__) || defined(unix)
#ifndef unix
#define unix
#endif
#include <errno.h>
#include <time.h>
#include <semaphore.h>
#endif
#if defined(WIN32) || defined(__CYGWIN__) && !defined unix
#if ( !defined(_WIN32_WINNT) || _WIN32_WINNT < 0x0400)
#pragma message "Unsupported platform, Windows NT 4.0 or later required"
#endif
#include <windows.h>
#endif

#ifdef _MSC_VER
#pragma warning( disable : 4100) // warning C4100: unreferenced formal parameter
#endif

namespace apache
{
  namespace ppr
  {
    namespace thread
    {

/*
 *
 */
class Semaphore
{
private:
#ifdef MACOSX
    // MacOSX lacks support for timedwait() on a semaphore. Need to build a semaphore upon a condition.
    pthread_cond_t cond;
    pthread_mutex_t mutex;
    int counter;
#elif defined unix
    sem_t semaphore ;
#else
    HANDLE semaphore ;
#endif

public:
    Semaphore(int initialSize = 0) ;
    ~Semaphore() ;

    void notify() ;
    void notify(int count) ;
    void wait() ;
    bool wait(int timeout_sec, time_t now = -1) ;
    bool trywait() ;
} ;

// Optimize all methods via inline code

inline Semaphore::Semaphore(int initialSize)
{
#ifdef MACOSX
  pthread_cond_init (&cond, NULL);
  pthread_mutex_init (&mutex, NULL);
  counter = initialSize;
#elif defined unix
  sem_init(&semaphore, 0 /* local to process */, initialSize) ;
#else 
  semaphore = CreateSemaphore(NULL, initialSize, 1000000, NULL) ;
#endif
}

inline Semaphore::~Semaphore()
{
#ifdef MACOSX
  pthread_mutex_destroy (&mutex);
  pthread_cond_destroy (&cond);
#elif defined unix
  sem_destroy(&semaphore) ;
#else
  CloseHandle(semaphore) ;
#endif
}

inline void Semaphore::notify()
{
#ifdef MACOSX
  pthread_mutex_lock (&mutex);
  ++counter;
  pthread_cond_signal (&cond);
  pthread_mutex_unlock (&mutex);
#elif defined unix
  sem_post(&semaphore) ;
#else
  ReleaseSemaphore(semaphore, 1, NULL) ;
#endif
}

inline void Semaphore::notify(int count)
{
#ifdef MACOSX
  pthread_mutex_lock (&mutex);
  counter += count;
  pthread_cond_signal (&cond);
  pthread_mutex_unlock (&mutex);
#elif defined unix
  while( count != 0 )
  {
    sem_post(&semaphore) ;
    --count ;
  }
#else
  ReleaseSemaphore(semaphore, count, NULL) ;
#endif
}

inline void Semaphore::wait()
{
#ifdef MACOSX
  pthread_mutex_lock (&mutex);
  while (counter == 0) {
    pthread_cond_wait (&cond, &mutex);
  }
  --counter;
  pthread_mutex_unlock (&mutex);
#elif defined unix
  sem_wait(&semaphore) ;
#else
  DWORD rc = WaitForSingleObject(semaphore, INFINITE) ;
  assert(rc == WAIT_OBJECT_0) ;
#endif
}

/*
 * Waits specified number of seconds until it gives up. Returns false
 * if semaphore is signaled, true when timeout is reached.
 */
inline bool Semaphore::wait(int timeout, time_t now)
{
#ifdef MACOSX
  if (now == -1) time(&now) ;
  timespec ts ;
  ts.tv_sec = now + timeout ;
  ts.tv_nsec = 0 ;

  pthread_mutex_lock (&mutex);
  while (counter == 0) {
    if (pthread_cond_timedwait (&cond, &mutex, &ts) == ETIMEDOUT) {
      pthread_mutex_unlock (&mutex);
      return true;
    }
  }
  --counter;
  pthread_mutex_unlock (&mutex);
  return true;
#elif defined unix
  if (now == -1)
    time(&now) ;

  timespec ts ;
  ts.tv_sec = now + timeout ;
  ts.tv_nsec = 0 ;

  do
  { 
    int rc = sem_timedwait(&semaphore, &ts) ;

    if (rc == 0)
      return false ;

    int errvalue = errno ;
      
    // Timeout occurred?
    if ( errvalue == ETIMEDOUT)
      return true ;

    assert(errvalue != EDEADLK) ;
    assert(errvalue != EINVAL) ;
  }
  while( true ) ;

  return true ;
#else
  DWORD rc = WaitForSingleObject(semaphore, timeout * 1000) ;
  return (rc == WAIT_OBJECT_0 ? false : true) ;
#endif
}

/*
 *  Returns false if some error occured or semaphore has zero count, true
 *  if semaphore successfully was decreased.
 */
inline bool Semaphore::trywait()
{
#ifdef MACOSX
  pthread_mutex_lock (&mutex);
  if (counter == 0) {
    pthread_mutex_unlock (&mutex);
    return false;
  } else {
    --counter;
    pthread_mutex_unlock (&mutex);
    return true;
  }
#elif defined unix
  int rc = sem_trywait(&semaphore) ;
  return ( rc == 0 ) ? true : false ;
#else
  DWORD rc = WaitForSingleObject(semaphore, 0) ;
  return (rc == WAIT_OBJECT_0 ? true : false) ;
#endif
}

/* namespace */
    }
  }
}

#endif /*Ppr_Semaphore_hpp_*/

