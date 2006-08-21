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

#ifndef THREADPOOLTEST_H_
#define THREADPOOLTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/concurrent/ThreadPool.h>
#include <activemq/concurrent/TaskListener.h>
#include <activemq/concurrent/Mutex.h>

namespace activemq{
namespace concurrent{

   class ThreadPoolTest : 
      public CppUnit::TestFixture,
      public TaskListener
   {
      CPPUNIT_TEST_SUITE( ThreadPoolTest );
      CPPUNIT_TEST( test1 );
      CPPUNIT_TEST( test2 );
      CPPUNIT_TEST_SUITE_END();

      int tasksToComplete;
      int complete;
      Mutex mutex;
      Mutex completeMutex;
      bool caughtEx;
      
   public:
 
   	ThreadPoolTest() 
      { 
         complete = 0;
         tasksToComplete = 0;
         caughtEx = false;
      }
      
   	virtual ~ThreadPoolTest() {};
      
      virtual void onTaskComplete(Runnable* task)
      {
        try{
             synchronized(&mutex)
             {
                complete++;
                
                if(tasksToComplete == complete)
                {
                   mutex.notifyAll();
                }
             }
        }catch( exceptions::ActiveMQException& ex ){
            ex.setMark( __FILE__, __LINE__ );
        }
      }

      virtual void onTaskException(Runnable* task, exceptions::ActiveMQException& ex)
      {
         caughtEx = true;
      }
      
   public:
   
      class MyTask : public Runnable
      {
      public:
      
         int value;
         
         MyTask(int x)
         {
            value = x;
         }
         
         virtual ~MyTask() {};
         
         virtual void run(void)
         {
            value += 100;
         }
      };

      class MyWaitingTask : public Runnable
      {
      public:
      
         Mutex* mutex;
         Mutex* complete;
         
         MyWaitingTask(Mutex* mutex, Mutex* complete)
         {
            this->mutex = mutex;
            this->complete = complete;
         }
         
         virtual ~MyWaitingTask() {};
         
         virtual void run(void)
         {
            try
            {
               synchronized(mutex)
               {
                  mutex->wait();
               }

               synchronized(complete)
               {
                   complete->notify();
               }
            }
            catch( exceptions::ActiveMQException& ex )
            {
                ex.setMark( __FILE__, __LINE__ );
            }
         }
      };

   public:

      void test2()
      {
         try
         {
            ThreadPool pool;
            Mutex myMutex;
    
            CPPUNIT_ASSERT( pool.getMaxThreads() == ThreadPool::DEFAULT_MAX_POOL_SIZE );
            CPPUNIT_ASSERT( pool.getBlockSize() == ThreadPool::DEFAULT_MAX_BLOCK_SIZE );
            pool.setMaxThreads(3);
            pool.setBlockSize(1);
            CPPUNIT_ASSERT( pool.getMaxThreads() == 3 );
            CPPUNIT_ASSERT( pool.getBlockSize() == 1 );
            CPPUNIT_ASSERT( pool.getPoolSize() == 0 );
            pool.reserve( 4 );
            CPPUNIT_ASSERT( pool.getPoolSize() == 3 );
            CPPUNIT_ASSERT( pool.getFreeThreadCount() == 3 );
    
            MyWaitingTask task1(&myMutex, &completeMutex);
            MyWaitingTask task2(&myMutex, &completeMutex);
            MyWaitingTask task3(&myMutex, &completeMutex);
            MyWaitingTask task4(&myMutex, &completeMutex);
    
            complete = 0;
            tasksToComplete = 4;
    
            pool.queueTask(ThreadPool::Task(&task1, this));
            pool.queueTask(ThreadPool::Task(&task2, this));
            pool.queueTask(ThreadPool::Task(&task3, this));
            pool.queueTask(ThreadPool::Task(&task4, this));
             
            Thread::sleep( 1000 );
             
            CPPUNIT_ASSERT( pool.getFreeThreadCount() == 0 );
            CPPUNIT_ASSERT( pool.getBacklog() == 1 );
    
            int count = 0;
            while(complete != tasksToComplete && count < 100)
            {
               synchronized(&myMutex)
               {
                  myMutex.notifyAll();
               }

               synchronized(&completeMutex)
               {
                  completeMutex.wait(1000);
               }

               count++;
            }
    
            CPPUNIT_ASSERT( complete == tasksToComplete );
            CPPUNIT_ASSERT( caughtEx == false );
         }
         catch( exceptions::ActiveMQException& ex )
         {
            ex.setMark( __FILE__, __LINE__ );
         }
      }
   
      void test1()
      {
         MyTask task1(1);
         MyTask task2(2);
         MyTask task3(3);
         
         complete = 0;
         tasksToComplete = 3;
         
         ThreadPool* pool = ThreadPool::getInstance();
         
         // Can't check this here since one of the other tests might
         // have used the global thread pool.
         // CPPUNIT_ASSERT( pool->getPoolSize() == 0 );

         pool->queueTask(ThreadPool::Task(&task1, this));
         pool->queueTask(ThreadPool::Task(&task2, this));
         pool->queueTask(ThreadPool::Task(&task3, this));
         
         Thread::sleep(500);
         
         CPPUNIT_ASSERT( complete == tasksToComplete );

         CPPUNIT_ASSERT( task1.value == 101 );
         CPPUNIT_ASSERT( task2.value == 102 );
         CPPUNIT_ASSERT( task3.value == 103 );
         
         CPPUNIT_ASSERT( pool->getPoolSize() > 0 );
         CPPUNIT_ASSERT( pool->getBacklog() == 0 );

         CPPUNIT_ASSERT( pool->getMaxThreads() == ThreadPool::DEFAULT_MAX_POOL_SIZE );
         CPPUNIT_ASSERT( pool->getBlockSize() == ThreadPool::DEFAULT_MAX_BLOCK_SIZE );

         pool->setMaxThreads(50);
         pool->setBlockSize(50);

         CPPUNIT_ASSERT( pool->getMaxThreads() == 50 );
         CPPUNIT_ASSERT( pool->getBlockSize() == 50 );

         Thread::sleep(500);

         CPPUNIT_ASSERT( pool->getFreeThreadCount() == pool->getPoolSize() );
         CPPUNIT_ASSERT( caughtEx == false );
         
      }
   };

}}

#endif /*THREADPOOLTEST_H_*/
