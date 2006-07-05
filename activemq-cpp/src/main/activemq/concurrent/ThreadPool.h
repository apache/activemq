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
#ifndef _ACTIVEMQ_CONCURRENT_THREADPOOL_H_
#define _ACTIVEMQ_CONCURRENT_THREADPOOL_H_

#include <activemq/concurrent/Runnable.h>
#include <activemq/concurrent/PooledThread.h>
#include <activemq/concurrent/PooledThreadListener.h>
#include <activemq/concurrent/TaskListener.h>
#include <activemq/concurrent/Mutex.h>
#include <activemq/util/Queue.h>
#include <activemq/logger/LoggerDefines.h>

#include <vector>

namespace activemq{
namespace concurrent{

   /**
    * Defines a Thread Pool object that implements the functionality
    * of pooling threads to perform user tasks.  The Thread Poll has
    * max size that it will grow to.  The thread pool allocates threads
    * in blocks.  When there are no waiting worker threads and a task
    * is queued then a new batch is allocated.  The user can specify
    * the size of the blocks, otherwise a default value is used.
    * <P>
    * When the user queues a task they must also queue a listner to 
    * be notified when the task has completed, this provides the user
    * with a mechanism to know when a task object can be freed.
    * <P>
    * To have the Thread Pool perform a task, the user enqueue's an
    * object that implements the <code>Runnable</code> insterface and
    * one of the worker threads will executing it in its thread context.
    */
   class ThreadPool : public PooledThreadListener
   {
   public:
   
      // Constants
      static const size_t DEFAULT_MAX_POOL_SIZE  = 10;
      static const size_t DEFAULT_MAX_BLOCK_SIZE = 3;
         
      // Types
      typedef std::pair<Runnable*, TaskListener*> Task;

   private:
   
      // Vector of threads that this object has created for its pool.
      std::vector< PooledThread* > pool;
      
      // Queue of Task that are in need of completion
      util::Queue<Task> queue;
      
      // Max number of Threads this Pool can contian      
      unsigned long maxThreads;
      
      // Max number of tasks that can be allocated at a time
      unsigned long blockSize;
      
      // boolean flag use to indocate that this object is shutting down.
      bool shutdown;
      
      // Count of threads that are currently free to perfom some work.
      unsigned long freeThreads;
      
      // Mutex for locking operations that affect the pool.
      Mutex poolLock;

      // Logger Init 
      LOGCMS_DECLARE(logger);
      LOGCMS_DECLARE(marker);
      
   private:   // Statics
   
      // The singleton instance of this class
      static ThreadPool instance;
            
   public:
         
      /**
       * Constructor
       */
      ThreadPool(void);

      /**
       * Destructor
       */
   	virtual ~ThreadPool(void);

      /**
       * Queue a task to be completed by one of the Pooled Threads.
       * tasks are serviced as soon as a <code>PooledThread</code>
       * is available to run it.
       * @param object that derives from Runnable
       * @throws ActiveMQException
       */
      virtual void queueTask(Task task) 
         throw ( exceptions::ActiveMQException );

      /**
       * DeQueue a task to be completed by one of the Pooled Threads.
       * A caller of this method will block until there is something
       * in the tasks queue, therefore care must be taken when calling
       * this function.  Normally clients of ThreadPool don't use
       * this, only the <code>PooledThread</code> objects owned by
       * this ThreadPool.
       * @return object that derives from Runnable
       * @throws ActiveMQException
       */
      virtual Task deQueueTask(void)
         throw ( exceptions::ActiveMQException );

      /**
       * Returns the current number of Threads in the Pool, this is
       * how many there are now, not how many are active or the max 
       * number that might exist.
       * @return integer number of threads in existance.
       */
      virtual unsigned long getPoolSize(void) const { return pool.size(); }
      
      /**
       * Returns the current backlog of items in the tasks queue, this
       * is how much work is still waiting to get done.  
       * @return number of outstanding tasks.
       */
      virtual unsigned long getBacklog(void) const { return queue.size(); }
      
      /**
       * Ensures that there is at least the specified number of Threads
       * allocated to the pool.  If the size is greater than the MAX
       * number of threads in the pool, then only MAX threads are 
       * reservved.  If the size is smaller than the number of threads
       * currently in the pool, than nothing is done.
       * @param number of threads to reserve.
       */
      virtual void reserve(unsigned long size);
      
      /**
       * Get the Max Number of Threads this Pool can contain
       * @return max size
       */
      virtual unsigned long getMaxThreads(void) const { return maxThreads; }
      
      /**
       * Sets the Max number of threads this pool can contian. 
       * if this value is smaller than the current size of the
       * pool nothing is done.
       */
      virtual void setMaxThreads(unsigned long maxThreads);
      
      /**
       * Gets the Max number of threads that can be allocated at a time
       * when new threads are needed.
       * @return max Thread Block Size
       */
      virtual unsigned long getBlockSize(void) const { return blockSize; }
      
      /**
       * Sets the Max number of Threads that can be allocated at a time
       * when the Thread Pool determines that more Threads are needed.  
       * @param Max Thread Block Size
       */
      virtual void setBlockSize(unsigned long blockSize);
      
      /**
       * Returns the current number of available threads in the pool, threads
       * that are performing a user task are considered unavailable.  This value
       * could change immeadiately after calling as Threads could finish right
       * after and be available again.  This is informational only.
       * @return totoal free threads
       */
      virtual unsigned long getFreeThreadCount(void) const { return freeThreads; }

   public: // PooledThreadListener Callbacks
      
      /**
       * Called by a pooled thread when it is about to begin
       * executing a new task.  This will decrement the available
       * threads counter so that this object knows when there are
       * no more free threads and must create new ones.
       * @param Pointer to the Pooled Thread that is making this call
       */
      virtual void onTaskStarted(PooledThread* thread);
       
      /**
       * Called by a pooled thread when it has completed a task
       * and is going back to waiting for another task to run,
       * this will increment the free threads counter.
       * @param Pointer the the Pooled Thread that is making this call.
       */
      virtual void onTaskCompleted(PooledThread* thread);

      /**
       * Called by a pooled thread when it has encountered an exception
       * while running a user task, after receiving this notification
       * the callee should assume that the PooledThread is now no longer
       * running.
       * @param Pointer to the Pooled Thread that is making this call
       * @param The Exception that occured.
       */
      virtual void onTaskException(PooledThread* thread, 
                                   exceptions::ActiveMQException& ex);

   public:   // Statics

      /**
       * Return the one and only Thread Pool instance.
       * @return The Thread Pool Pointer
       */
      static ThreadPool* getInstance(void) { return &instance; }

   private:
   
      /**
       * Allocates the requested ammount of Threads, won't exceed
       * <code>maxThreads</code>.
       * @param the number of threads to create
       */
      void AllocateThreads(unsigned long count); 

   };

}}

#endif /*_ACTIVEMQ_CONCURRENT_THREADPOOL_H_*/
