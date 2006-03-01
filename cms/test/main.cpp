#include <activemq/ActiveMQTextMessage.h>
#include <cms/TopicConnectionFactory.h>
#include <cms/TopicConnection.h>
#include <cms/TopicSession.h>
#include <cms/TopicSubscriber.h>
#include <cms/Topic.h>
#include <activemq/ActiveMQConnectionFactory.h>
#include <cms/MessageListener.h>
#include <cms/ExceptionListener.h>
#include <cms/TopicPublisher.h>

#include <stdlib.h>
#include <sys/time.h>
#include <iostream>

using namespace std;

class Tester 
: 
    public cms::ExceptionListener,
    public cms::MessageListener
{
public:

    Tester(){numReceived = 0;}
    virtual ~Tester(){}
    
    void test(){
        
        try{
        	
        	int numMessages = 1000;
        	int sleepTime = 10;
        	
        	printf("Starting activemqcms test (sending %d messages and sleeping %d seconds) ...\n", numMessages, sleepTime );
            
            cms::TopicConnectionFactory* connectionFactory = new activemq::ActiveMQConnectionFactory( "127.0.0.1:61626" );
            cms::TopicConnection* connection = connectionFactory->createTopicConnection();
            connection->setExceptionListener( this );
            connection->start();
            cms::TopicSession* session = connection->createTopicSession( false );
            cms::Topic* topic = session->createTopic("mytopic");
            cms::TopicSubscriber* subscriber = session->createSubscriber( topic );            
            subscriber->setMessageListener( this );
            cms::TopicPublisher* publisher = session->createPublisher( topic );
            
            const char* text = "this is a test!";
            cms::TextMessage* msg = session->createTextMessage( text );
                        
            for( int ix=0; ix<numMessages; ++ix ){                
                publisher->publish( msg );
                
                
                timespec sleepTime;
                sleepTime.tv_sec = 0;
                sleepTime.tv_nsec = 1000;
                nanosleep( &sleepTime, &sleepTime );
            }
            
            sleep( sleepTime );
            
            printf("received: %d\n", numReceived );
            
            delete publisher;
            subscriber->close();
            delete subscriber;
            session->close();
            delete session;
            connection->close();
            delete connection;
            delete connectionFactory;
            
        }catch( cms::CMSException& ex ){
            printf("StompTester::test() - %s\n", ex.getMessage() );
        }
    }
    
    virtual void onMessage( const cms::Message* message ){
        const cms::TextMessage* txtMsg = dynamic_cast<const cms::TextMessage*>(message);
        if( txtMsg == NULL ){
            printf("received non-text message\n" );
            return;
        }
        
        numReceived++;
        //printf( "[%d]: %s\n", ++ix, txtMsg->getMessage() );
    }
    
    virtual void onException( const cms::CMSException* error ){
        printf( "StompTester::onException() - %s\n", error->getMessage() );
    }
    
private:
    
    int numReceived;
};

int main(int argc, char *argv[]){
	try{
        
        Tester tester;
        tester.test();
		
	}catch( ... ){
		printf("main - caught unknown exception\n" );		
	}
	
	printf("done");
    
    cout.flush();
    
	return 0;
}

