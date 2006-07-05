#ifndef ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAMTEST_H_
#define ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAMTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/io/ByteArrayOutputStream.h>

namespace activemq{
namespace io{

   class ByteArrayOutputStreamTest : public CppUnit::TestFixture 
   {
     CPPUNIT_TEST_SUITE( ByteArrayOutputStreamTest );
     CPPUNIT_TEST( testStream );
     CPPUNIT_TEST_SUITE_END();

   public:

   	ByteArrayOutputStreamTest() {}

   	virtual ~ByteArrayOutputStreamTest() {}

      void testStream()
      {
         ByteArrayOutputStream stream_a;
         
         stream_a.write('a');
         stream_a.write(60);
         stream_a.write('c');
         
         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 3 );
         
         stream_a.clear();
         
         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 0 );
         
         stream_a.write((const unsigned char*)("abc"), 3);
  
         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 3 );
         
         stream_a.clear();

         CPPUNIT_ASSERT( stream_a.getByteArraySize() == 0 );
         
         stream_a.write((const unsigned char*)("abc"), 3);

         unsigned char buffer[4];
         
         memset(buffer, 0, 4);
         memcpy(buffer, stream_a.getByteArray(), stream_a.getByteArraySize());
         
         CPPUNIT_ASSERT( std::string((const char*)buffer) == std::string("abc") );
      }
   };

}}

#endif /*ACTIVEMQ_IO_BYTEARRAYOUTPUTSTREAMTEST_H_*/
