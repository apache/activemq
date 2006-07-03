#ifndef ACTIVEMQ_IO_BUFFEREDINPUTSTREAMTEST_H_
#define ACTIVEMQ_IO_BUFFEREDINPUTSTREAMTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/io/BufferedInputStream.h>


namespace activemq{
namespace io{
	
	class BufferedInputStreamTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( BufferedInputStreamTest );
	  CPPUNIT_TEST( testSmallerBuffer );
	  CPPUNIT_TEST( testBiggerBuffer );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class MyInputStream : public InputStream{
		private:
			std::string data;
			unsigned int pos;
		public:
		
			MyInputStream( const std::string& data ){
				this->data = data;
				pos = 0;
			}
			virtual ~MyInputStream(){}
			
			virtual int available() const{
				int len = data.length();
				return len - (int)pos;
			}
			virtual unsigned char read() throw (IOException){
				if( pos >= data.length() ){
					throw IOException();
				}
				
				return data.c_str()[pos++];
			}
			virtual int read( unsigned char* buffer, const int bufferSize ) throw (IOException){
				unsigned int numToRead = std::min( bufferSize, available() );
				
				const char* str = data.c_str();
				for( unsigned int ix=0; ix<numToRead; ++ix ){
					buffer[ix] = str[pos+ix];
				}
				
				pos += numToRead;
				
				return numToRead;
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
			
			std::string testStr = "TEST12345678910";
			MyInputStream myStream( testStr );
			BufferedInputStream bufStream( &myStream, 1 );
			
			int available = bufStream.available();
			CPPUNIT_ASSERT( available == (int)testStr.length() );
			
			unsigned char dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'T' );
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == ((int)testStr.length() - 1 ) );
			
			dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'E' );
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == ((int)testStr.length() - 2 ) );
			
			dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'S' );
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == ((int)testStr.length() - 3 ) );
			
			dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'T' );
			
			unsigned char dummyBuf[20];
			memset( dummyBuf, 0, 20 );
			int numRead = bufStream.read( dummyBuf, 10 );
			CPPUNIT_ASSERT( numRead == 10 );
			CPPUNIT_ASSERT( strcmp( (char*)dummyBuf, "1234567891" ) == 0 );			
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == 1 );
		}
		
		void testBiggerBuffer(){
			
			std::string testStr = "TEST12345678910";
			MyInputStream myStream( testStr );
			BufferedInputStream bufStream( &myStream, 10 );
			
			int available = bufStream.available();
			CPPUNIT_ASSERT( available == (int)testStr.length() );
			
			unsigned char dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'T' );
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == ((int)testStr.length() - 1 ) );
			
			dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'E' );
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == ((int)testStr.length() - 2 ) );
			
			dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'S' );
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == ((int)testStr.length() - 3 ) );
			
			dummy = bufStream.read();
			CPPUNIT_ASSERT( dummy == 'T' );
			
			unsigned char dummyBuf[20];
			memset( dummyBuf, 0, 20 );
			int numRead = bufStream.read( dummyBuf, 10 );
			CPPUNIT_ASSERT( numRead == 10 );
			CPPUNIT_ASSERT( strcmp( (char*)dummyBuf, "1234567891" ) == 0 );			
			
			available = bufStream.available();
			CPPUNIT_ASSERT( available == 1 );
		}
	};
	
}}

#endif /*ACTIVEMQ_IO_BUFFEREDINPUTSTREAMTEST_H_*/
