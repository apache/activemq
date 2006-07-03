#ifndef _ACTIVEMQ_UTIL_GUIDTEST_H_
#define _ACTIVEMQ_UTIL_GUIDTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/util/Guid.h>

namespace activemq{
namespace util{

   class GuidTest : public CppUnit::TestFixture 
   {
     CPPUNIT_TEST_SUITE( GuidTest );
     CPPUNIT_TEST( test );
     CPPUNIT_TEST_SUITE_END();

   public:
   
   	virtual ~GuidTest() {}
   
      void test(void)
      {
         util::Guid guid; 
   
         guid.createGUID();
         
         CPPUNIT_ASSERT( guid.toString() == (std::string)guid );

         Guid copy = guid;

         CPPUNIT_ASSERT( guid == copy );
         CPPUNIT_ASSERT( !(guid < copy) );
         CPPUNIT_ASSERT( guid <= copy );
         CPPUNIT_ASSERT( !(guid > copy) );
         CPPUNIT_ASSERT( guid >= copy );

         std::string less = "0f2bd21c-9fee-4067-d739-c4d84a5d7f62";
         std::string more = "1f2bd21c-9fee-4067-d739-c4d84a5d7f62";

         CPPUNIT_ASSERT( less < more );
         CPPUNIT_ASSERT( less <= more );
         CPPUNIT_ASSERT( !(less > more) );
         CPPUNIT_ASSERT( !(less >= more) );

         less = more;
         
         CPPUNIT_ASSERT( less == more );
   
         const unsigned char* bytes = guid.toBytes();

         Guid bytesGUID;
         bytesGUID.fromBytes(bytes);

         CPPUNIT_ASSERT( guid == bytesGUID );

         delete bytes;

         Guid bytesGUID2;
         bytesGUID2.fromBytes((const unsigned char*)guid);
         
         CPPUNIT_ASSERT( guid == bytesGUID2 );
   
         Guid stringGUID(guid.toString());
   
         CPPUNIT_ASSERT( stringGUID == guid );

         Guid stringGUID2(guid.toString().c_str());
   
         CPPUNIT_ASSERT( stringGUID2 == guid );
         CPPUNIT_ASSERT( !(stringGUID2 != guid) );

      }
   };

}}

#endif /*_ACTIVEMQ_UTIL_GUIDTEST_H_*/
