#ifndef ACTIVEMQ_CONCURRENT_MUTEXTEST_H_
#define ACTIVEMQ_CONCURRENT_MUTEXTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/concurrent/Concurrent.h>
#include <activemq/concurrent/Thread.h>
#include <activemq/concurrent/Mutex.h>
#include <time.h>

namespace activemq{
namespace concurrent{

    class MutexTest : public CppUnit::TestFixture {

        CPPUNIT_TEST_SUITE( MutexTest );
        CPPUNIT_TEST( test );
        CPPUNIT_TEST( testWait );
        CPPUNIT_TEST( testTimedWait );
        CPPUNIT_TEST( testNotify );
        CPPUNIT_TEST( testNotifyAll );
        CPPUNIT_TEST( testRecursiveLock );
        CPPUNIT_TEST( testDoubleLock );
        CPPUNIT_TEST_SUITE_END();

    public:

        class MyThread 
        : 
            public Thread,
            public Synchronizable{
    
        private:
        
            Mutex mutex;
            
        public:
        
            int value;
            MyThread(){ value = 0;}
            virtual ~MyThread(){}
            
            virtual void lock() throw(exceptions::ActiveMQException){
                mutex.lock();
            }    
            virtual void unlock() throw(exceptions::ActiveMQException){
                mutex.unlock();
            }       
            virtual void wait() throw(exceptions::ActiveMQException){
                mutex.wait();
            }
            virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
                mutex.wait( millisecs );
            }  
            virtual void notify() throw(exceptions::ActiveMQException){
                mutex.notify();
            }
            virtual void notifyAll() throw(exceptions::ActiveMQException){
                mutex.notifyAll();
            }            
        
            virtual void run(){
                
                {
                    Lock lock (this);
                    
                    value = value * 25;                 
                }
            }
        
        };
      
        class MyWaitingThread 
        : 
            public Thread,
            public Synchronizable{
         
        private:
      
            Mutex mutex;
         
        public:
     
            int value;
            MyWaitingThread(){ value = 0;}
            virtual ~MyWaitingThread(){}
            virtual void lock() throw(exceptions::ActiveMQException){
                mutex.lock();
            }    
            virtual void unlock() throw(exceptions::ActiveMQException){
                mutex.unlock();
            }       
            virtual void wait() throw(exceptions::ActiveMQException){
                mutex.wait();
            }
            virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
                mutex.wait( millisecs );
            }
            virtual void notify() throw(exceptions::ActiveMQException){
                mutex.notify();
            }
            virtual void notifyAll() throw(exceptions::ActiveMQException){
                mutex.notifyAll();
            }  
         
            virtual void run(){
         
                try
                {
                    synchronized(this)            
                    {
                        this->wait();
            
                        std::cout.flush();
                 
                        value = value * 25;              
                    }
                }
                catch(exceptions::ActiveMQException& ex)
                {
                    ex.setMark( __FILE__, __LINE__ );
                }
            }
      };

      class MyTimedWaitingThread 
      : 
         public Thread,
         public Synchronizable{
        
      private:
    
         Mutex mutex;
         
      public:
     
         int value;
         MyTimedWaitingThread(){ value = 0;}
         virtual ~MyTimedWaitingThread(){}
         virtual void lock() throw(exceptions::ActiveMQException){
             mutex.lock();
         }    
         virtual void unlock() throw(exceptions::ActiveMQException){
             mutex.unlock();
         }       
         virtual void wait() throw(exceptions::ActiveMQException){
             mutex.wait();
         }
         virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
             mutex.wait( millisecs );
         }
         virtual void notify() throw(exceptions::ActiveMQException){
             mutex.notify();
         }
         virtual void notifyAll() throw(exceptions::ActiveMQException){
             mutex.notifyAll();
         }  
        
         virtual void run(){
      
            try
            {
               synchronized(this)            
               {
                  this->wait(2000);
                        
                  value = 666;              
               }
            }
            catch(exceptions::ActiveMQException& ex)
            {
                ex.setMark( __FILE__, __LINE__ );
            }
         }
      };
     
      class MyNotifiedThread 
      : 
         public Thread,
         public Synchronizable{
        
      public:
      
         bool done;
         Mutex* mutex;
         
      public:
     
         int value;
         MyNotifiedThread(Mutex* mutex){ this->mutex = mutex; done = false; }
         virtual ~MyNotifiedThread(){}
         virtual void lock() throw(exceptions::ActiveMQException){
             mutex->lock();
         }    
         virtual void unlock() throw(exceptions::ActiveMQException){
             mutex->unlock();
         }       
         virtual void wait() throw(exceptions::ActiveMQException){
             mutex->wait();
         }
         virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
             mutex->wait( millisecs );
         }
         virtual void notify() throw(exceptions::ActiveMQException){
             mutex->notify();
         }
         virtual void notifyAll() throw(exceptions::ActiveMQException){
             mutex->notifyAll();
         }  
        
         virtual void run(){
      
            try
            {
               done = false;
               synchronized(this)            
               {                  
                  this->wait();
                  done = true;
               }
            }
            catch(exceptions::ActiveMQException& ex)
            {
               ex.setMark( __FILE__, __LINE__ );
            }
         }
      };
      
      class MyRecursiveLockThread 
      : 
         public Thread,
         public Synchronizable{
        
      public:
      
         bool done;
         Mutex* mutex;
         
      public:
     
         int value;
         MyRecursiveLockThread(Mutex* mutex){ this->mutex = mutex; done = false; }
         virtual ~MyRecursiveLockThread(){}
         virtual void lock() throw(exceptions::ActiveMQException){
             mutex->lock();
         }    
         virtual void unlock() throw(exceptions::ActiveMQException){
             mutex->unlock();
         }       
         virtual void wait() throw(exceptions::ActiveMQException){
             mutex->wait();
         }
         virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
             mutex->wait( millisecs );
         }
         virtual void notify() throw(exceptions::ActiveMQException){
             mutex->notify();
         }
         virtual void notifyAll() throw(exceptions::ActiveMQException){
             mutex->notifyAll();
         }  
        
         virtual void run(){
      
            try
            {
               done = false;
               synchronized(this)            
               {
                  synchronized(this)
                  {
                     this->wait();
                     done = true;
                  }
               }
            }
            catch(exceptions::ActiveMQException& ex)
            {
               ex.setMark( __FILE__, __LINE__ );
            }
         }
      };

      class MyDoubleLockThread 
      : 
         public Thread
      {
        
      public:
      
         bool done;
         Mutex* mutex1;
         Mutex* mutex2;
         
      public:
     
         int value;
         MyDoubleLockThread(Mutex* mutex1, Mutex* mutex2)
         {
            this->mutex1 = mutex1;
            this->mutex2 = mutex2;
            done = false;
         }

         virtual ~MyDoubleLockThread(){}
        
         virtual void run(){
      
            try
            {
               done = false;
               synchronized(mutex1)            
               {
                  synchronized(mutex2)
                  {
                     mutex2->wait();
                     done = true;
                  }
               }
            }
            catch(exceptions::ActiveMQException& ex)
            {
               ex.setMark( __FILE__, __LINE__ );
            }
         }
      };

   public:
    
        virtual void setUp(){}; 
        virtual void tearDown(){};
        
      void testTimedWait(){

         try
         {
            MyTimedWaitingThread test;
            time_t startTime = time( NULL );
            test.start();
            test.join();
            time_t endTime = time( NULL );
       
            long delta = endTime - startTime;

            CPPUNIT_ASSERT( delta >= 1 && delta <= 3 );
        }
        catch(exceptions::ActiveMQException& ex)
        {
           std::cout << ex.getMessage() << std::endl;
        }
      }

        void testWait(){

         try
         {
            MyWaitingThread test;
            test.start();

            Thread::sleep(1000);

            synchronized(&test)
            {
                for( int ix=0; ix<100; ix++ ){
                    test.value += 1;
                }
  
               test.notify();
            }
   
            test.join();
          
            CPPUNIT_ASSERT( test.value == 2500 );

        }
        catch(exceptions::ActiveMQException& ex)
        {
           ex.setMark( __FILE__, __LINE__ );
        }
        }
      
        void test()
        {
            MyThread test;
            test.lock();

            test.start();
         
            for( int ix=0; ix<100; ix++ ){
                test.value += 1;
            }
         
            test.unlock();
            test.join();
         
            CPPUNIT_ASSERT( test.value == 2500 );
        }
      
        void testNotify()
        {
            try{
                Mutex mutex;
                const int numThreads = 30;
                MyNotifiedThread* threads[numThreads];
             
                // Create and start all the threads.
                for( int ix=0; ix<numThreads; ++ix ){
                    threads[ix] = new MyNotifiedThread( &mutex );
                    threads[ix]->start();
                }
             
                // Sleep so all the threads can get to the wait.
                Thread::sleep( 1000 );
             
                synchronized(&mutex)
                {
                    mutex.notify();
                }
             
                Thread::sleep( 1000 );
             
                int counter = 0;
                for( int ix=0; ix<numThreads; ++ix ){
                    if( threads[ix]->done ){
                        counter++;
                    }
                }
             
                // Make sure only 1 thread was notified.
                CPPUNIT_ASSERT( counter == 1 ); 
             
                synchronized(&mutex)
                {
                    // Notify all threads.
                    for( int ix=0; ix<numThreads-1; ++ix ){
                        mutex.notify();
                    }
                }
             
                // Sleep to give the threads time to wake up.
                Thread::sleep( 1000 );
             
                int numComplete = 0;
                for( int ix=0; ix<numThreads; ++ix ){
                    if( threads[ix]->done ){
                        numComplete++;
                    }  
                }
                CPPUNIT_ASSERT( numComplete == numThreads );
                
                synchronized( &mutex )
                {
                    mutex.wait( 5 );
                }
                
                synchronized( &mutex )
                {
                    mutex.notifyAll();
                }
                                
            }catch( exceptions::ActiveMQException& ex ){
                ex.setMark( __FILE__, __LINE__ );
            }
        }
      
        void testNotifyAll()
        {
            try{
                Mutex mutex;
             
                const int numThreads = 100;
                MyNotifiedThread* threads[numThreads];
             
                // Create and start all the threads.
                for( int ix=0; ix<numThreads; ++ix ){
                    threads[ix] = new MyNotifiedThread( &mutex );
                    threads[ix]->start();
                }
             
                // Sleep so all the threads can get to the wait.
                Thread::sleep( 1000 );
             
                for( int ix=0; ix<numThreads; ++ix )
                {
                    if( threads[ix]->done == true ){
                        printf("threads[%d] is done prematurely\n", ix );
                    }
                    CPPUNIT_ASSERT( threads[ix]->done == false );            
                }
             
                // Notify all threads.
                synchronized( &mutex ){
                   mutex.notifyAll();
                }
             
                // Sleep to give the threads time to wake up.
                Thread::sleep( 1000 );
             
                int numComplete = 0;
                for( int ix=0; ix<numThreads; ++ix ){
                    if( threads[ix]->done ){
                        numComplete++;
                    }  
                }
                printf("numComplete: %d, numThreads: %d\n", numComplete, numThreads );
                CPPUNIT_ASSERT( numComplete == numThreads );
             
            }catch( exceptions::ActiveMQException& ex ){
                ex.setMark( __FILE__, __LINE__ );
            }
        }

        void testRecursiveLock()
        {
            try{
                Mutex mutex;
             
                const int numThreads = 30;
                MyRecursiveLockThread* threads[numThreads];
             
                // Create and start all the threads.
                for( int ix=0; ix<numThreads; ++ix ){
                    threads[ix] = new MyRecursiveLockThread( &mutex );
                    threads[ix]->start();
                }
             
                // Sleep so all the threads can get to the wait.
                Thread::sleep( 1000 );
             
                for( int ix=0; ix<numThreads; ++ix ){
                    if( threads[ix]->done == true ){
                        std::cout << "threads[" << ix 
                                  << "] is done prematurely\n";
                    }
                    CPPUNIT_ASSERT( threads[ix]->done == false );            
                }
             
                // Notify all threads.
                synchronized( &mutex )
                {
                    synchronized( &mutex )
                    {
                        mutex.notifyAll();
                    }
                }
             
                // Sleep to give the threads time to wake up.
                Thread::sleep( 1000 );
             
                for( int ix=0; ix<numThreads; ++ix ){
                    if( threads[ix]->done != true ){
                        std::cout<< "threads[" << ix << "] is not done\n";
                    }
                    CPPUNIT_ASSERT( threads[ix]->done == true );            
                }
            }catch( exceptions::ActiveMQException& ex ){
                ex.setMark( __FILE__, __LINE__ );
            }
        }

        void testDoubleLock()
        {
            try{
                Mutex mutex1;
                Mutex mutex2;

                MyDoubleLockThread thread(&mutex1, &mutex2);
    
                thread.start();
    
                // Let the thread get both locks
                Thread::sleep( 200 );
    
                // Lock mutex 2, thread is waiting on it
                synchronized(&mutex2)
                {
                   mutex2.notify();
                }
    
                // Let the thread die
                thread.join();
    
                CPPUNIT_ASSERT( thread.done );
            }catch( exceptions::ActiveMQException& ex ){
                ex.setMark( __FILE__, __LINE__ );
            }
        }
    };
    
}}

#endif /*ACTIVEMQ_CONCURRENT_MUTEXTEST_H_*/
