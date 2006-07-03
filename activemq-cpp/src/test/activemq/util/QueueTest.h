#ifndef _ACTIVEMQ_UTIL_QUEUETEST_H_
#define _ACTIVEMQ_UTIL_QUEUETEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/Queue.h>

namespace activemq{
namespace util{

   class QueueTest : public CppUnit::TestFixture {

     CPPUNIT_TEST_SUITE( QueueTest );
     CPPUNIT_TEST( test );
     CPPUNIT_TEST_SUITE_END();

   public:

   	virtual ~QueueTest() {}
    
      void test()
      {
         Queue<char> q;
         
         CPPUNIT_ASSERT( q.empty() == true );
         CPPUNIT_ASSERT( q.size() == 0 );

         q.push('a');
    
         CPPUNIT_ASSERT( q.front() == 'a' );
         
         q.pop();
         
         CPPUNIT_ASSERT( q.empty() == true );
         
         q.push('b');
         q.push('c');
         
         CPPUNIT_ASSERT( q.size() == 2 );
         
         CPPUNIT_ASSERT( q.front() == 'b' );
         CPPUNIT_ASSERT( q.back() == 'c' );

      }
   };
    
}}

#endif /*_ACTIVEMQ_UTIL_QUEUETEST_H_*/
