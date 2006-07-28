/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
#include <activemq/ActiveMQBytesMessage.h>
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
        	
        	int messagesPerType = 3000;
        	int sleepTime = 2;
        	
        	printf("Starting activemqcms test (sending %d messages per type and sleeping %d seconds) ...\n", messagesPerType, sleepTime );
            
            // START SNIPPET: demo
            cms::TopicConnectionFactory* connectionFactory = new activemq::ActiveMQConnectionFactory( "127.0.0.1:61613" );
            cms::TopicConnection* connection = connectionFactory->createTopicConnection();
            connection->setExceptionListener( this );
            connection->start();
            cms::TopicSession* session = connection->createTopicSession( false );
            cms::Topic* topic = session->createTopic("mytopic");
            cms::TopicSubscriber* subscriber = session->createSubscriber( topic );            
            subscriber->setMessageListener( this );
            cms::TopicPublisher* publisher = session->createPublisher( topic );
            
            // Send some text messages.
            const char* text = "this is a test!";
            cms::TextMessage* textMsg = session->createTextMessage( text );                        
            for( int ix=0; ix<messagesPerType; ++ix ){                
                publisher->publish( textMsg );
                doSleep();               
            }
            
            // Send some bytes messages.
            char buf[10];
            memset( buf, 0, 10 );
            buf[0] = 0;
            buf[1] = 1;
            buf[2] = 2;
            buf[3] = 3;
            buf[4] = 0;
            buf[5] = 4;
            buf[6] = 5;
            buf[7] = 6;
            cms::BytesMessage* bytesMsg = session->createBytesMessage();
            bytesMsg->setData( buf, 10 );
            for( int ix=0; ix<messagesPerType; ++ix ){                
                publisher->publish( bytesMsg ); 
                doSleep();               
            }        
            // END SNIPPET: demo
            
            sleep( sleepTime );
            
            printf("received: %d\n", numReceived );
            
            sleep( 5 );
            
            printf("unsubscribing\n" );
            delete publisher;                      
            subscriber->close();
            delete subscriber;
            
            sleep( 5 );            
            
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
        
        // Got a text message.
        const cms::TextMessage* txtMsg = dynamic_cast<const cms::TextMessage*>(message);
        if( txtMsg != NULL ){
            //printf("received text msg: %s\n", txtMsg->getText() );
        }
        
        // Got a bytes msg.
        const cms::BytesMessage* bytesMsg = dynamic_cast<const cms::BytesMessage*>(message);
        if( bytesMsg != NULL ){
            /*printf("received bytes msg: " );
            const char* bytes = bytesMsg->getData();
            int numBytes = bytesMsg->getNumBytes();
            for( int ix=0; ix<numBytes; ++ix ){
            	printf("[%d]", bytes[ix] );
            }
            printf("\n");*/
        }
        
        numReceived++;
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

