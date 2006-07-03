#ifndef ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAMTEST_H_
#define ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAMTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/io/BufferedOutputStream.h>
#include <string.h>

namespace activemq{
namespace io{
	
	class BufferedOutputStreamTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( BufferedOutputStreamTest );
	  CPPUNIT_TEST( testSmallerBuffer );
	  CPPUNIT_TEST( testBiggerBuffer );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class MyOutputStream : public OutputStream{
		private:
			char buffer[100];
			unsigned int pos;
		public:
		
			MyOutputStream(){
				pos = 0;
				memset( buffer, 0, 100 );
			}
			virtual ~MyOutputStream(){}
			
			const char* getBuffer() const{ return buffer; }
			
			virtual void write( const unsigned char c ) throw (IOException){
				if( pos >= 100 ){
					throw IOException();
				}
				
				buffer[pos++] = c;
			}
		
			virtual void write( const unsigned char* buffer, const int len ) throw (IOException){
				
				if( (pos + len) > 100 ){
					throw IOException();
				}
				
				memcpy( this->buffer + pos, buffer, len );
				
				pos += len;
			}
		
			virtual void flush() throw (IOException){
			}

			virtual void close() throw(cms::CMSException){
				// do nothing.
			}
			
		    virtual void lock() throw(exceptions::ActiveMQException){
            }
            virtual void unlock() throw(exceptions::ActiveMQException){
            }
            virtual void wait() throw(exceptions::ActiveMQException){
            }
            virtual void wait(unsigned long millisecs) throw(exceptions::ActiveMQException){
            }
            virtual void notify() throw(exceptions::ActiveMQException){
            }
            virtual void notifyAll() throw(exceptions::ActiveMQException){
            }
		};
		
	public:
	
		virtual void setUp(){};	
	 	virtual void tearDown(){};
		void testSmallerBuffer(){
			
			MyOutputStream myStream;
			BufferedOutputStream bufStream( &myStream, 1 );
			
			const char* buffer = myStream.getBuffer();
			
			bufStream.write( (unsigned char)'T' );
			// Should not be written yet.
			CPPUNIT_ASSERT( strcmp( buffer, "" ) == 0 );
			
			bufStream.write( (unsigned char)'E' );
			// This time the T should have been written.
			CPPUNIT_ASSERT( strcmp( buffer, "T" ) == 0 );
			
			bufStream.write( (unsigned char*)"ST", 2 );
			// This time the ES should have been written.
			CPPUNIT_ASSERT( strcmp( buffer, "TES" ) == 0 );
			
			bufStream.flush();
			CPPUNIT_ASSERT( strcmp( buffer, "TEST" ) == 0 );			
		}
		
		void testBiggerBuffer(){
			
			MyOutputStream myStream;
			BufferedOutputStream bufStream( &myStream, 10 );
			
			const char* buffer = myStream.getBuffer();
			
			bufStream.write( (unsigned char*)"TEST", 4 );
			
			// Should not be written yet.
			CPPUNIT_ASSERT( strcmp( buffer, "" ) == 0 );
			
			bufStream.flush();
			CPPUNIT_ASSERT( strcmp( buffer, "TEST" ) == 0 );
			
			bufStream.write( (unsigned char*)"TEST", 4 );
			bufStream.write( (unsigned char*)"12345678910", 11);
			
			CPPUNIT_ASSERT( strcmp( buffer, "TESTTEST123456" ) == 0 );
			
			bufStream.flush();
			CPPUNIT_ASSERT( strcmp( buffer, "TESTTEST12345678910" ) == 0 );
			
		}
	};
	
}}

#endif /*ACTIVEMQ_IO_BUFFEREDOUTPUTSTREAMTEST_H_*/
