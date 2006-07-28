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

#ifndef ACTIVEMQ_CONCURRENT_MUTEX_H
#define ACTIVEMQ_CONCURRENT_MUTEX_H

// Includes.
#include <activemq/concurrent/Synchronizable.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Thread.h>
#include <list>

#if (defined(__unix__) || defined(unix) || defined(MACOSX)) && !defined(USG)
    #ifndef unix
        #define unix
    #endif
   
    #include <pthread.h>
    #include <sys/time.h>
#endif

#if defined(WIN32) || defined(__CYGWIN__) && !defined unix
   
    #include <windows.h>
   
    #if ( !defined(_WIN32_WINNT) || _WIN32_WINNT < 0x0400)
    #if ( !defined(WINVER) || WINVER < 0x0400)
        #pragma message ("Unsupported platform, Windows NT 4.0 or later required")
    #endif
    #endif

#endif

#include <assert.h>

namespace activemq{
namespace concurrent{
    
    /**
     * Creates a pthread_mutex_t object. The object is created
     * such that successive locks from the same thread is allowed
     * and will be successful.
     * @see  pthread_mutex_t
     */
    class Mutex : public Synchronizable
    {
    private:       // Data

        /**
         * The mutex object.
         */
        #ifdef unix
            pthread_mutex_t mutex;

            std::list<pthread_cond_t*> eventQ;
        #else
            CRITICAL_SECTION mutex;

            std::list<HANDLE> eventQ;
        #endif
      
        // Lock Status Members
        int           lock_count;
        unsigned long lock_owner;

    public:

        /**
         * Constructor - creates and initializes the mutex.
         */
        Mutex()
        {
            #ifdef unix
                pthread_mutexattr_t attr;
                pthread_mutexattr_init(&attr);
                pthread_mutex_init(&mutex, &attr);
                pthread_mutexattr_destroy(&attr);
            #else
                InitializeCriticalSection(&mutex);            
            #endif
          
            lock_owner = 0;
            lock_count = 0;
        }

        /**
         * Destructor - destroys the mutex object.
         */
        virtual ~Mutex()
        {
            // Unlock the mutex.
            unlock();
      
            #ifdef unix
                pthread_mutex_destroy(&mutex);
            #else
                DeleteCriticalSection(&mutex);
            #endif
        }
      
        /**
         * Locks the object.
         */
        virtual void lock() throw( exceptions::ActiveMQException )
        {
            if(isLockOwner())
            {
                lock_count++;
            }
            else
            {
                #ifdef unix               
                    pthread_mutex_lock(&mutex);
                #else
                    EnterCriticalSection(&mutex);
                #endif

                lock_count = 1;
                lock_owner = Thread::getId();
            }         
        }
      
        /**
         * Unlocks the object.
         */
        virtual void unlock() throw( exceptions::ActiveMQException )
        {
            if(lock_owner == 0)
            {
                return;
            }
         
            if(!isLockOwner())
            {
                throw exceptions::ActiveMQException( 
                    __FILE__, __LINE__,
                    "Mutex::unlock - Failed, not Lock Owner!" );
            }
         
            lock_count--;
         
            if(lock_count == 0)
            {         
                lock_owner = 0;

                #ifdef unix
                    pthread_mutex_unlock(&mutex);
                #else
                    LeaveCriticalSection(&mutex);
                #endif            
            }
        }
      
        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.
         */
        virtual void wait() throw( exceptions::ActiveMQException )
        {
            // Delegate to the timed version
            wait( WAIT_INFINITE );
        }

        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.  This wait will timeout after the specified time
         * interval.
         */
        virtual void wait( unsigned long millisecs ) 
            throw( exceptions::ActiveMQException )
        {
            if(!isLockOwner())
            {
                throw exceptions::ActiveMQException( 
                    __FILE__, __LINE__,
                    "Mutex::wait - Failed, not Lock Owner!");
            }

            // Save the current owner and Lock count as we are going to 
            // unlock and release for someone else to lock on potentially.
            // When we come back and re-lock we want to restore to the 
            // state we were in before.
            unsigned long lock_owner = this->lock_owner;
            int           lock_count = this->lock_count;

            this->lock_count = 0;
            this->lock_owner = 0;
         
            #ifdef unix

                // Create this threads wait event
                pthread_cond_t waitEvent;
                pthread_cond_init(&waitEvent, NULL);
            
                // Store the event in the queue so that a notify can
                // call it and wake up the thread.
                eventQ.push_back(&waitEvent);

                int returnValue = 0;
                if(millisecs != WAIT_INFINITE)
                {
                    timeval now = {};
                    gettimeofday(&now, NULL);
               
                    timespec wait = {};
                    wait.tv_sec = now.tv_sec + (millisecs / 1000);
                    wait.tv_nsec = (now.tv_usec * 1000) + ((millisecs % 1000) * 1000000);
               
                    if(wait.tv_nsec > 1000000000)
                    {
                        wait.tv_sec++;
                        wait.tv_nsec -= 1000000000;
                    }

                    returnValue =  pthread_cond_timedwait(&waitEvent, &mutex, &wait);
                }
                else
                {
                    returnValue = pthread_cond_wait(&waitEvent, &mutex);
                }

                // If the wait did not succeed for any reason, remove it
                // from the queue.
                if( returnValue != 0 ){
                    std::list<pthread_cond_t*>::iterator iter = eventQ.begin();
                    for( ; iter != eventQ.end(); ++iter ){
                        if( *iter == &waitEvent ){
                            eventQ.erase(iter);
                            break;
                        }
                    }
                }
                
                // Destroy our wait event now, the notify method will have removed it
                // from the event queue.
                pthread_cond_destroy(&waitEvent);

            #else

                // Create the event to wait on
                HANDLE waitEvent = CreateEvent( NULL, false, false, NULL );
            
                if(waitEvent == NULL)
                {
                    throw exceptions::ActiveMQException( 
                        __FILE__, __LINE__,
                        "Mutex::Mutex - Failed Creating Event." );
                }

                eventQ.push_back( waitEvent );

                // Release the Lock
                LeaveCriticalSection( &mutex );

                // Wait for a signal
                WaitForSingleObject( waitEvent, millisecs );

                // Reaquire the Lock
                EnterCriticalSection( &mutex );

                // Clean up the event, the notif methods will have
                // already poped it from the queue.
                CloseHandle( waitEvent );
            
            #endif
         
            // restore the owner
            this->lock_owner = lock_owner;
            this->lock_count = lock_count;
        }
      
        /**
         * Signals a waiter on this object that it can now wake
         * up and continue.
         */
        virtual void notify() throw( exceptions::ActiveMQException )
        {
            if( !isLockOwner() )
            {
                throw exceptions::ActiveMQException( 
                    __FILE__, __LINE__,
                    "Mutex::Notify - Failed, not Lock Owner!" );
            }

            if( !eventQ.empty() )
            {
                #ifdef unix
                    pthread_cond_signal( eventQ.front() );
                    eventQ.pop_front();
                #else
                    SetEvent( eventQ.front() );
                    eventQ.pop_front();
                #endif
            }
        }

        /**
         * Signals the waiters on this object that it can now wake
         * up and continue.
         */
        virtual void notifyAll() throw( exceptions::ActiveMQException )
        {
            if(!isLockOwner())
            {
                throw exceptions::ActiveMQException( 
                    __FILE__, __LINE__,
                    "Mutex::NotifyAll - Failed, not Lock Owner!" );
            }
         
            #ifdef unix

                while(!eventQ.empty())
                {
                     pthread_cond_signal( eventQ.front() );
                     eventQ.pop_front();
                }

            #else

                while(!eventQ.empty())
                {
                     SetEvent( eventQ.front() );
                     eventQ.pop_front();
                }

            #endif
        }

    private:
   
        /**
         * Check if the calling thread is the Lock Owner
         */
        bool isLockOwner()
        {
            return lock_owner == Thread::getId();
        }
      
    };

}}

#endif // ACTIVEMQ_CONCURRENT_MUTEX_H
