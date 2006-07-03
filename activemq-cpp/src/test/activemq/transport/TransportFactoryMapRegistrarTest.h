#ifndef ACTIVEMQ_TRANSPORT_CONNECTORFACTORYMAPREGISTRARTEST_H_
#define ACTIVEMQ_TRANSPORT_CONNECTORFACTORYMAPREGISTRARTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/transport/TransportFactoryMap.h>
#include <activemq/transport/TransportFactoryMapRegistrar.h>

namespace activemq{
namespace transport{
	
	class TransportFactoryMapRegistrarTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( TransportFactoryMapRegistrarTest );
	  CPPUNIT_TEST( test );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class TestTransportFactory : public TransportFactory
		{
		public:
		
		   virtual Transport* createTransport(
		      const activemq::util::Properties& properties ) { return NULL; };
		};
		
		void test(){
			
			{
				TransportFactoryMapRegistrar registrar("Test", new TestTransportFactory());
			
				CPPUNIT_ASSERT( TransportFactoryMap::getInstance().lookup("Test") != NULL);
			}
			
			CPPUNIT_ASSERT( TransportFactoryMap::getInstance().lookup( "Test" ) == NULL );
		}
	};
	
}}

#endif /*ACTIVEMQ_TRANSPORT_CONNECTORFACTORYMAPREGISTRARTEST_H_*/
