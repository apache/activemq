#ifndef ACTIVEMQ_TRANSPORT_TRANSPORTFACTORYMAPTEST_H_
#define ACTIVEMQ_TRANSPORT_TRANSPORTFACTORYMAPTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/transport/TransportFactoryMap.h>
#include <activemq/transport/TransportFactory.h>

namespace activemq{
namespace transport{
	
	class TransportFactoryMapTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( TransportFactoryMapTest );
	  CPPUNIT_TEST( test );
	  CPPUNIT_TEST_SUITE_END();	  
		
	public:
	
		class TestTransportFactory : public TransportFactory
		{
		public:
		
		   virtual Transport* createTransport(
		      const activemq::util::Properties& properties) { return NULL; };
		};
		
		void test(){
			
			TransportFactoryMap& factMap = 
                TransportFactoryMap::getInstance();
			TestTransportFactory testFactory;
			
			factMap.registerTransportFactory( "test", &testFactory );
			
			CPPUNIT_ASSERT( factMap.lookup( "test" ) == &testFactory );
			
			std::vector<std::string> names;
			CPPUNIT_ASSERT( factMap.getFactoryNames( names ) >= 1 );
			
            bool found = false;
            for( unsigned int i = 0; i < names.size(); ++i )
            {
                if( names[i] == "test" )
                {
                    found = true;
                    break;
                }
            }
			CPPUNIT_ASSERT( found );
			
			factMap.unregisterTransportFactory( "test" );
			CPPUNIT_ASSERT( factMap.lookup( "test" ) == NULL );			
		}
	};
	
}}

#endif /*ACTIVEMQ_TRANSPORT_TRANSPORTFACTORYMAPTEST_H_*/
