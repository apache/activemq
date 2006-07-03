#ifndef ACTIVEMQ_CONNECTOR_CONNECTORFACTORYMAPTEST_H_
#define ACTIVEMQ_CONNECTOR_CONNECTORFACTORYMAPTEST_H_

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <activemq/connector/ConnectorFactoryMap.h>
#include <activemq/connector/SessionInfo.h>
#include <activemq/connector/TransactionInfo.h>
#include <cms/Topic.h>
#include <cms/Queue.h>
#include <cms/TemporaryTopic.h>
#include <cms/TemporaryQueue.h>

namespace activemq{
namespace connector{
	
	class ConnectorFactoryMapTest : public CppUnit::TestFixture {
		
	  CPPUNIT_TEST_SUITE( ConnectorFactoryMapTest );
	  CPPUNIT_TEST( test );
	  CPPUNIT_TEST_SUITE_END();
	  
	public:
	
		class testConnector : public Connector
		{
		public:
		
		      virtual SessionInfo* createSessionInfo(void) throw( ConnectorException )
		      { return NULL; };
		
		      virtual cms::Topic* createTopic(const std::string& name, SessionInfo* session)
		          throw ( ConnectorException )
		      { return NULL; };
		      virtual cms::Queue* createQueue(const std::string& name, SessionInfo* session)
		         throw ( ConnectorException )
		      { return NULL; };
		          
		      virtual cms::TemporaryTopic* createTemporaryTopic(const std::string& name, 
		                                                    SessionInfo*       session)
		         throw ( ConnectorException )
		     { return NULL; };
		         
		       virtual cms::TemporaryQueue* createTemporaryQueue(const std::string& name, 
		                                                    SessionInfo*       session)
		          throw ( ConnectorException )
		       { return NULL; };
		          
		      virtual void Send(cms::Message* message) throw ( ConnectorException ) {};
		      virtual void Send(std::list<cms::Message*>& messages) 
		         throw ( ConnectorException ) {};
            virtual void Acknowledge(cms::Message* message) throw ( ConnectorException ) {};
		      virtual TransactionInfo* startTransaction(SessionInfo* session) 
		         throw ( ConnectorException ) { return NULL; };
		      virtual void commit(TransactionInfo* transaction, SessionInfo* session)
		         throw ( ConnectorException ) {};
		      virtual void rollback(TransactionInfo* transaction, SessionInfo* session)
		         throw ( ConnectorException ) {};
		
		      virtual cms::BytesMessage* createByteMessage(SessionInfo*     session,
		                                             TransactionInfo* transaction)
		                                               throw ( ConnectorException )
		      { return NULL; };
		      virtual cms::TextMessage* createTextMessage(SessionInfo*     session,
		                                             TransactionInfo* transaction)
		                                               throw ( ConnectorException )
		      { return NULL; };
		      virtual void subscribe(cms::Destination* destination, SessionInfo* session)
		         throw ( ConnectorException ) {};
		      virtual void unsubscribe(cms::Destination* destination, SessionInfo* session)
		         throw ( ConnectorException ) {};
		      virtual void addMessageListener(cms::MessageListener* listener) {};
		      virtual void removeMessageListener(cms::MessageListener* listener) {};
            virtual void addExceptionListener(cms::ExceptionListener* listener) {};
            virtual void removeExceptionListener(cms::ExceptionListener* listener) {};
		
		};
		
	public:
	
		class TestConnectoryFactory : public ConnectorFactory
		{
		public:
		
		   virtual Connector* createConnector(
		      const activemq::util::Properties& properties,
            activemq::transport::Transport*   transport) { return NULL; };
		};
		
		void test(){
			
			ConnectorFactoryMap* factMap = ConnectorFactoryMap::getInstance();
			CPPUNIT_ASSERT( factMap != NULL );
			
			TestConnectoryFactory testFactory;
			
			factMap->registerConnectorFactory( "test", &testFactory );
			
			CPPUNIT_ASSERT( factMap->lookup( "test" ) == &testFactory );
			
			std::vector<std::string> names;
			CPPUNIT_ASSERT( factMap->getFactoryNames( names ) >= 1 );
			
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
			
			factMap->unregisterConnectorFactory( "test" );
			CPPUNIT_ASSERT( factMap->lookup( "test" ) == NULL );			
		}
	};
	
}}

#endif /*ACTIVEMQ_CONNECTOR_CONNECTORFACTORYMAPTEST_H_*/
