#ifndef LOGGERTEST_H_
#define LOGGERTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <activemq/logger/LoggerDefines.h>

namespace activemq{
namespace logger{

   class LoggerTest : public CppUnit::TestFixture 
   {
     CPPUNIT_TEST_SUITE( LoggerTest );
     CPPUNIT_TEST( test );
     CPPUNIT_TEST_SUITE_END();

   private:
   
      LOGCMS_DECLARE(testLogger);
      
   public:

   	virtual ~LoggerTest() {}

      void test(void)
      {
         LOGCMS_DEBUG(testLogger, "Test Debug");
         LOGCMS_INFO(testLogger, "Test Info");
         LOGCMS_ERROR(testLogger, "Test Error");
         LOGCMS_WARN(testLogger, "Test Warn");
         LOGCMS_FATAL(testLogger, "Test Fatal");

         CPPUNIT_ASSERT( true );
      }
   };

   LOGCMS_INITIALIZE(testLogger, LoggerTest, "com.activemq.logger.LoggerTest");

}}

#endif /*LOGGERTEST_H_*/
