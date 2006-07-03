#ifndef ACTIVEMQ_CONNECTOR_CONNECTORFACTORYMAPREGISTRARTEST_H_
#define ACTIVEMQ_CONNECTOR_CONNECTORFACTORYMAPREGISTRARTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/ConnectorFactoryMap.h>
#include <activemq/connector/ConnectorFactoryMapRegistrar.h>

namespace activemq{
namespace connector{
	
	class ConnectorFactoryMapRegistrarTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( ConnectorFactoryMapRegistrarTest );
	  CPPUNIT_TEST( test );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class TestConnectoryFactory : public ConnectorFactory
		{
		public:
		
		   virtual Connector* createConnector(
		      const activemq::util::Properties& properties,
            activemq::transport::Transport*   transport) { return NULL; };
		};
		
		void test(){
			
			{
				ConnectorFactoryMapRegistrar registrar("Test", new TestConnectoryFactory());
			
				CPPUNIT_ASSERT( ConnectorFactoryMap::getInstance()->lookup("Test") != NULL);
			}
			
			CPPUNIT_ASSERT( ConnectorFactoryMap::getInstance()->lookup( "Test" ) == NULL );
		}
	};
	
}}

#endif /*ACTIVEMQ_CONNECTOR_CONNECTORFACTORYMAPREGISTRARTEST_H_*/
