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
