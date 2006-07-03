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
#include <activemq/concurrent/ThreadPool.h>
#include <activemq/concurrent/Concurrent.h>
#include <activemq/exceptions/IllegalArgumentException.h>

#ifdef min
#undef min
#endif

#include <algorithm>
#include <iostream>

using namespace std;
using namespace activemq;
using namespace activemq::concurrent;

////////////////////////////////////////////////////////////////////////////////
LOGCMS_INITIALIZE(logger, ThreadPool, "com.activemq.concurrent.ThreadPool");
LOGCMS_INITIALIZE(marker, ThreadPool, "com.activemq.concurrent.ThreadPool.Marker");

////////////////////////////////////////////////////////////////////////////////
ThreadPool ThreadPool::instance;

////////////////////////////////////////////////////////////////////////////////
ThreadPool::ThreadPool(void)
{
   maxThreads  = DEFAULT_MAX_POOL_SIZE;
   blockSize   = DEFAULT_MAX_BLOCK_SIZE;
   freeThreads = 0;

   shutdown = false;
}

////////////////////////////////////////////////////////////////////////////////
ThreadPool::~ThreadPool(void)
{
   try
   {
      std::vector<PooledThread*>::iterator itr = pool.begin();
       
      // Stop all the threads
      for(; itr != pool.end(); ++itr)
      {
         (*itr)->stop();
      }
       
      // Set the shutdown flag so that the DeQueue methods all quit
      // when we interrupt them.
      shutdown = true;
       
      synchronized(&queue)
      {
         // Signal the Queue so that all waiters are notified
         queue.notifyAll();
      }
       
      // Wait for everyone to die
      for(itr = pool.begin(); itr != pool.end(); ++itr)
      {
         (*itr)->join();
      }      
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ThreadPool::queueTask(ThreadPool::Task task) 
   throw ( exceptions::ActiveMQException )
{
   try
   {
      if(!task.first || !task.second)
      {
         throw exceptions::IllegalArgumentException( __FILE__, __LINE__,
            "ThreadPool::QueueTask - Invalid args for Task");
      }
       
      //LOGCMS_DEBUG(logger, "ThreadPool::QueueTask - syncing on queue");
    
      synchronized(&queue)
      {
         //LOGCMS_DEBUG(logger, "ThreadPool::QueueTask - sync'd, synching pool");
    
         // If there's nobody open to do work, then create some more
         // threads to handle the work.
         if(freeThreads == 0)
         {
            AllocateThreads(blockSize);
         }
    
         //LOGCMS_DEBUG(logger, "ThreadPool::QueueTask - pushing task");
    
         // queue the new work.
         queue.push(task);
         
         //LOGCMS_DEBUG(logger, "ThreadPool::QueueTask - calling notify");
   
         // Inform waiters that we put some work on the queue.
         queue.notify();
      }
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
ThreadPool::Task ThreadPool::deQueueTask(void)
   throw ( exceptions::ActiveMQException )
{
   try
   {
      //LOGCMS_DEBUG(logger, "ThreadPool::DeQueueTask - syncing on queue");
    
      synchronized(&queue)
      {
         /*LOGCMS_DEBUG(logger, 
            "ThreadPool::DeQueueTask - sync'd checking queue empty");*/

         // Wait for work, wait in a while loop since another thread could
         // be waiting for a lock and get the work before we get woken up
         // from our wait.
         while(queue.empty() && !shutdown)
         {
            //LOGCMS_DEBUG(logger, "ThreadPool::DeQueueTask - Q empty, waiting");

            queue.wait();

            //LOGCMS_DEBUG(logger, "ThreadPool::DeQueueTask - done waiting");
         }
          
         // Don't give more work if we are closing down
         if(shutdown)
         {
            return Task();
         }
          
         // check size again.
         if(queue.empty())
         {
            throw exceptions::ActiveMQException( __FILE__, __LINE__,
               "ThreadPool::DeQueueUserWorkItem - Empty Taskn, not in shutdown.");
         }
          
         //LOGCMS_DEBUG(logger, "ThreadPool::DeQueueTask - popping task");

         // not empty so get the new work to do
         return queue.pop();
      }
       
      return Task();
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ThreadPool::reserve(unsigned long size)
{
   try{
      synchronized(&poolLock)
      {
         if(size < pool.size() || pool.size() == maxThreads)
         {
            return;
         }
         
         // How many do we reserve
         unsigned long allocCount = size - pool.size();
          
         // Allocate the new Threads
         AllocateThreads(allocCount);
      }
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ThreadPool::setMaxThreads(unsigned long maxThreads)
{
   try
   {
      synchronized(&poolLock)
      {
         if(maxThreads == 0)
         {
            // Caller tried to do something stupid, ignore them.
            return;
         }
          
         this->maxThreads = maxThreads;
      }
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ThreadPool::setBlockSize(unsigned long blockSize)
{
   try
   {
      if(blockSize <= 0)
      {
         // User tried something dumb, protect them from themselves
         return;
      }
    
      synchronized(&poolLock)
      {
         this->blockSize = blockSize;
      }
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ThreadPool::AllocateThreads(unsigned long count)
{
   try
   {
      if(pool.size() >= maxThreads)
      {
         return;
      }
    
      synchronized(&poolLock)
      {
         // Take the min of alloc size of maxThreads since we don't
         // want anybody sneaking eaxtra threads in, greedy bastards.
         count = std::min(count, maxThreads - pool.size());
       
         // Each time we create a thread we increment the free Threads 
         // counter, but before we call start so that the Thread doesn't 
         // get ahead of us.
         for(unsigned long i = 0; i < count; ++i)
         {
            pool.push_back(new PooledThread(this));
            pool.back()->setPooledThreadListener(this);
            freeThreads++;
            pool.back()->start();
         }
      }
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ThreadPool::onTaskStarted(PooledThread* thread)
{
   try
   {
      synchronized(&poolLock)
      {
         freeThreads--;
          
         // Now that this callback has decremented the free threads coutner
         // let check if there is any outstanding work to be done and no
         // threads to handle it.  This could happen if the QueueTask
         // method was called successively without any of the PooledThreads
         // having a chance to wake up and service the queue.  This would
         // cause the number of Task to exceed the number of free threads
         // once the Threads got a chance to wake up and service the queue
         if(freeThreads == 0 && !queue.empty())
         {
            // Allocate a new block of threads
            AllocateThreads(blockSize);
         }
      }

      //LOGCMS_DEBUG(logger, "ThreadPool::onTaskStarted:");
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}
 
////////////////////////////////////////////////////////////////////////////////
void ThreadPool::onTaskCompleted(PooledThread* thread)
{
   try
   {    
      synchronized(&poolLock)
      {
         freeThreads++;
      }

      //LOGCMS_DEBUG(logger, "ThreadPool::onTaskCompleted: ");
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}

////////////////////////////////////////////////////////////////////////////////
void ThreadPool::onTaskException(
   PooledThread* thread, 
   exceptions::ActiveMQException& ex)
{
   //LOGCMS_DEBUG(logger, "ThreadPool::onTaskException: ");

   try
   {
      synchronized(&poolLock)
      {
         // Delete the thread that had the exception and start a new 
         // one to take its place.
         freeThreads--;
          
         std::vector<PooledThread*>::iterator itr = 
            std::find(pool.begin(), pool.end(), thread);
    
         if(itr != pool.end())
         {
            pool.erase(itr);
         }
    
         // Bye-Bye Thread Object
         delete thread;
          
         // Now allocate a replacement
         AllocateThreads(1);
      }
   }
   AMQ_CATCH_RETHROW( exceptions::ActiveMQException )
   AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
}
                                
