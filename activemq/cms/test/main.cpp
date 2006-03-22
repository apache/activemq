/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
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
            
            // START SNIPPET: demo
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
                doSleep();
            }
            // END SNIPPET: demo
            
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
    
    virtual void doSleep() {
        timespec sleepTime;
        sleepTime.tv_sec = 0;
        sleepTime.tv_nsec = 1000;
        nanosleep( &sleepTime, &sleepTime );
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

