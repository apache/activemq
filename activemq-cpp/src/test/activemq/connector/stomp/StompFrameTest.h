#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMETEST_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMETEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/stomp/StompFrame.h>

namespace activemq{
namespace connector{
namespace stomp{

   class StompFrameTest : public CppUnit::TestFixture
   {

     CPPUNIT_TEST_SUITE( StompFrameTest );
     CPPUNIT_TEST( test );
     CPPUNIT_TEST_SUITE_END();

   public:

   	virtual ~StompFrameTest() {}

      void test()
      {
         StompFrame frame;
         
         CPPUNIT_ASSERT( frame.getCommand() == "" );
         frame.setCommand("test");
         CPPUNIT_ASSERT( frame.getCommand() == "test" );
                  
         frame.getProperties().setProperty("key", "value");
         
         std::string result = frame.getProperties().getProperty("key");
         
         CPPUNIT_ASSERT( result == "value" );         
         
         CPPUNIT_ASSERT( frame.getBody() == NULL );
         CPPUNIT_ASSERT( frame.getBodyLength() == 0 );
         
         frame.setBody( strdup("ABC"), 4 );
         
         CPPUNIT_ASSERT( frame.getBody() != NULL );
         CPPUNIT_ASSERT( frame.getBodyLength() == 4 );
         CPPUNIT_ASSERT( std::string(frame.getBody()) == "ABC" );
      }
      
   };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPFRAMETEST_H_*/
