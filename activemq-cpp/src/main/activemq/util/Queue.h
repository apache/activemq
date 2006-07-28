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
#ifndef ACTIVEMQ_UTIL_QUEUE_H
#define ACTIVEMQ_UTIL_QUEUE_H

#include <queue>
#include <activemq/concurrent/Mutex.h>
#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace util{

    /**
     * The Queue class accepts messages with an psuh(m) command 
     * where m is the message to be queued.  It destructively 
     * returns the message with pop().  pop() returns messages in
     * the order they were enqueued.                                               
     *                                                              
     * Queue is implemented with an instance of the STL queue object.
     * The interface is essentially the same as that of the STL queue
     * except that the pop method actually reaturns a reference to the
     * element popped.  This frees the app from having to call the
     * <code>front</code> method before calling pop.
     *
     *  Queue<string> sq;     // make a queue to hold string messages
     *  sq.push(s);           // enqueues a message m
     *  string s = sq.pop();  // dequeues a message
     *
     * = DESIGN CONSIDERATIONS
     *
     * The Queue class inherits from the Synchronizable interface and
     * provides methods for locking and unlocking this queue as well as
     * waiting on this queue.  In a multi-threaded app this can allow
     * for multiple threads to be reading from and writing to the same
     * Queue.
     *
     * Clients should consider that in a multiple threaded app it is 
     * possible that items could be placed on the queue faster than
     * you are taking them off, so protection should be placed in your
     * polling loop to ensure that you don't get stuck there.
     */

    template <typename T> class Queue : public concurrent::Synchronizable
    {
    public:
   
        Queue(void);
        virtual ~Queue(void);

        /**
         * Returns a Reference to the element at the head of the queue
         * @return reference to a queue type object or (safe)
         */
        T& front(void);

        /**
         * Returns a Reference to the element at the head of the queue
         * @return reference to a queue type object or (safe)
         */
        const T& front(void) const;

        /**
         * Returns a Reference to the element at the tail of the queue
         * @return reference to a queue type object or (safe)
         */
        T& back(void);

        /**
         * Returns a Reference to the element at the tail of the queue
         * @return reference to a queue type object or (safe)
         */
        const T& back(void) const;

        /**
         * Places a new Object at the Tail of the queue
         * @param Queue Object Type reference.
         */
        void push( const T &t );

        /**
         * Removes and returns the element that is at the Head of the queue
         * @return reference to a queue type object or (safe)
         */
        T pop(void);

        /**
         * Gets the Number of elements currently in the Queue
         * @return Queue Size
         */
        size_t size(void) const;

        /**
         * Checks if this Queue is currently empty
         * @return boolean indicating queue emptiness
         */
        bool empty(void) const;
   
        /**
         * Locks the object.
         */
        virtual void lock() throw( exceptions::ActiveMQException ){
            mutex.lock();
        }
   
        /**
         * Unlocks the object.
         */
        virtual void unlock() throw( exceptions::ActiveMQException ){   
            mutex.unlock();
        }
       
        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.
         */
        virtual void wait() throw( exceptions::ActiveMQException ){
            mutex.wait();
        }
    
        /**
         * Waits on a signal from this object, which is generated
         * by a call to Notify.  Must have this object locked before
         * calling.  This wait will timeout after the specified time
         * interval.
         * @param time in millisecsonds to wait, or WAIT_INIFINITE
         * @throws ActiveMQException
         */
        virtual void wait( unsigned long millisecs ) 
            throw( exceptions::ActiveMQException ) {
         
            mutex.wait(millisecs);
        }

        /**
         * Signals a waiter on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         */
        virtual void notify() throw( exceptions::ActiveMQException ){
            mutex.notify();
        }
        
        /**
         * Signals the waiters on this object that it can now wake
         * up and continue.  Must have this object locked before
         * calling.
         */
        virtual void notifyAll() throw( exceptions::ActiveMQException ){
            mutex.notifyAll();
        }
   
    public:   // Statics

        /**
         * Fetch a reference to the safe value this object will return
         * when there is nothing to fetch from the queue.
         * @return Reference to this Queues safe object
         */
        static const T& getSafeValue(void) { return safe; }

    private:

        // The real queue
        std::queue<T> queue;

        // Object used for sync
        concurrent::Mutex mutex;

        // Safe value used when pop, front or back are
        // called and the queue is empty.
        static T safe;
      
    };
   
    //-----{ Static Init }----------------------------------------------------//
    template <typename T>
    T Queue<T>::safe;
   
    //-----{ Retrieve current length of Queue }-------------------------------//
   
    template <typename T> inline size_t Queue<T>::size() const
    {
        return queue.size();
    }
   
    //-----{ Retrieve whether Queue is empty or not }-------------------------//
   
    template <typename T> inline bool Queue<T>::empty(void) const
    {
        return queue.empty();
    }
   
    //-----{ Defulat Constructor }--------------------------------------------//
   
    template <typename T> 
    Queue<T>::Queue()
    {
    }

    //-----{ Default Destructor }---------------------------------------------//
   
    template <typename T> Queue<T>::~Queue()
    {
    }

    //-----{ Add Elements to Back of Queue }----------------------------------//

    template <typename T> 
    void Queue<T>::push( const T &t )
    {
        queue.push( t );
    }

    //-----{ Remove Elements from Front of Queue }----------------------------//

    template <typename T> 
    T Queue<T>::pop(void)
    {
        if(queue.empty())
        {   
            return safe;
        }

        // Pop the element into a temp, since we need to remain locked.
        // this means getting front and then popping.
        T temp = queue.front();
        queue.pop();
   
        return temp;
    }

    //-----{ Returnreference to element at front of Queue }-------------------//

    template <typename T> 
    T& Queue<T>::front(void)
    {
        if( queue.empty() )
        {
            return safe;
        }
         
        return queue.front();
    }
   
    //-----{ Returnreference to element at front of Queue }-------------------//

    template <typename T> 
    const T& Queue<T>::front(void) const
    {
        if( queue.empty() )
        {   
            return safe;
        }

        return queue.front();
    }
   
    //-----{ Returnreference to element at back of Queue }--------------------//

    template <typename T> 
    T& Queue<T>::back(void)
    {
        if( queue.empty() )
        {
            return safe;
        }
         
        return queue.back();
    }
   
    //-----{ Returnreference to element at back of Queue }--------------------//

    template <typename T> 
    const T& Queue<T>::back(void) const
    {
        if( queue.empty() )
        {
            return safe;
        }
   
        return queue.back();
    }

}}

#endif /* ACTIVEMQ_UTIL_QUEUE_H */
