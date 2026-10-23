---
title: "CMS API Overview"
sidebar_position: 2
---
# What is CMS?

The CMS API is a C++ corollary to the JMS API in Java which is used to send and receive messages from clients spread out across a network or located on the same machine.  In CMS we've made every attempt to maintain as much parity with the JMS api as possible, diverging only when a JMS feature depended strongly on features in the Java programming language itself.  Even though there are some differences most are quite minor and for the most part CMS adheres to the JMS spec, so having a firm grasp on how JMS works should make using CMS that much easier. 

One of the first places to start if you are already familiar with JMS is to take a look at the CMS [API documentation](/components/cms/documentation#api)

# Getting Started with CMS

This section covers the basics of using the CMS API. In order to aid in the discussion we assume here that you are using ActiveMQ-CPP to connect to an ActiveMQ Broker, of course with CMS you could also link to another implementation of the CMS API in your C++ application and connect to some other Message service.

## The CMS ConnectionFactory

The first interface you will generally use in the CMS API is the ConnectionFactory. A ConnectionFactory allows you to create CMS Connection objects which maintain a connection the some Messaging service, e.g. an ActiveMQ broker.

The simplest way to obtain an instance of a CMS ConnectionFactory is to use the static method **createCMSConnectionFactory** that all CMS Provider libraries are required to implement. The code snippet below demonstrates how to obtain a new ConnectionFactory:

```
std::auto_ptr<cms::ConnectionFactory> connectionFactory(
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );
```

As you can see the **createCMSConnectionFactory** takes a single string parameter which is in the form of a URI that defines where the Connection that is created is to connect to as well as the protocol that should be used, TCP/IP in the case of the above example. Additionally configuration information can be encoded in the URI. Refer to the [Configuration](/components/cms/configuring) page for more information on the configuration parameters that can be passed to ActiveMQ-CPP via the URI.

Once you've created a ConnectionFactory the next thing to do is to create a CMS Connection using the ConnectionFactory. A Connection is the Object that manages the client's connection to the Provider. The next section covers the use of a CMS Connection, the code to create a Connection is shown below:

```
std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );
```

Upon creation the Connection object attempts to connect to the CMS Provider, if the connection fails then an CMSException is thrown with a description of the error that occurred stored in its Message property.

### Connections and Authentication

There are a couple versions of the **createConnection** method that allow you to specify login parameters for the newly created Connection. The one you would use most often takes a user-name and password pair that is transmitted to the Broker for authentication. Should the credentials be invalid a CMSException would be thrown. The sample code below shows how to pass a user-name and password in when creating your Connection object.

```
std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection( "<USERNAME>", "<PASSWORD>") );
```

If you don't want to hard code values into your source code or write code to read the login data from somewhere else there is another option for passing in the user-name and password, the URI that you pass to **createConnectionFactory** can be encoded so that the connectionFactory will read the values from the system environment when it parses the URI. The example below shows how to create a connection factory with the login data set in the URI.

```
std::auto_ptr<cms::ConnectionFactory> connectionFactory(
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616?username=${USERNAME}&password=${PASSWORD}" ) );
```

As you can see it is pretty simple to have values on the URI come from the system environment. This method will work for any parameter that you can specify in the URI.

## CMS Connection

The CMS Connection interface defines an object that is the client's active connection to the CMS provider. In most cases the client will only create one connection object since it is considered a heavyweight object.

A connection serves several purposes:

*   It encapsulates an open connection with a JMS provider. It typically represents an open TCP/IP socket between a client and a provider service daemon.
*   Its creation is where client authentication takes place.
*   It can specify a unique client identifier.
*   It provides a ConnectionMetaData object.
*   It supports an optional ExceptionListener object.

A CMS Connection is created from a CMS ConnectionFactory as we saw previously. If the ConnectionFactory create call is successful then the Connection object returned is connected to the CMS Provider. The Connection object is created in a stopped state, no messages will be delivered to Message consumers that a client creates until the Connection is started. Its normal to leave a Connection in the stopped state until the client has created the initial set of Sessions, Message Producers, and Message Consumers that it intends to use. Once the setup phase of the client completes it should call the Connection's start method to begin receiving messages from the Provider. Failing to call the **start** method is a very common error among new users of CMS and JMS clients, if you find you aren't receiving any messages the first thing to check is that you called **start**.

After creating the Connection the client must create a CMS Session in order to create message producers and consumers. The code snippet below puts together what we've seen so far and then shows how to create a CMS Session object from our Connection instance, the section that follows will discuss the CMS Session in more detail.

```
std::auto_ptr<cms::ConnectionFactory> connectionFactory( 
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );

std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );

std::auto_ptr<cms::Session> session( connection->createSession() );
```

## CMS Session

Once you've successfully created a CMS Connection the next thing you would normall do is create one or more CMS Session objects using your new Connectio instance.  A Session is defined as a Single Threaded context for producing and consuming messages. 

A session serves several purposes:

*   It is a factory for its message producers and consumers.
*   It supplies provider-optimized message factories.
*   It is a factory for TemporaryTopics and TemporaryQueues.
*   It provides a way to create Queue or Topic objects for those clients that need to dynamically manipulate provider-specific destination names.
*   It supports a single series of transactions that combine work spanning its producers and consumers into atomic units.
*   It defines a serial order for the messages it consumes and the messages it produces.
*   It retains messages it consumes until they have been acknowledged.
*   It serializes execution of message listeners registered with its message consumers.
*   It is a factory for QueueBrowsers *(Not yet Implemented)*.

A session can create and service multiple message producers and consumers.

When a client creates a CMS session it must specify the mode in which the Session will acknowledge the messages that it receives and dispatches.  The modes supported are summarized in the table below.

|Acknowledge Mode|Description|
|---|---|
|AUTO_ACKNOWLEDGE|With this acknowledgement mode, the session automatically acknowledges a client's receipt of a message either when the session has successfully returned from a call to receive or when the message listener the session has called to process the message successfully returns.|
|CLIENT_ACKNOWLEDGE|With this acknowledgement mode, the client acknowledges a consumed message by calling the message's acknowledge method. Acknowledging a consumed message acknowledges all messages that the session has consumed. When client acknowledgement mode is used, a client may build up a large number of unacknowledged messages while attempting to process them. A CMS provider should provide administrators with a way to limit client overrun so that clients are not driven to resource exhaustion and ensuing failure when some resource they are using is temporarily blocked.|
|DUPS_OK_ACKNOWLEDGE|This acknowledgement mode instructs the session to lazily acknowledge the delivery of messages. This is likely to result in the delivery of some duplicate messages if the JMS provider fails, so it should only be used by consumers that can tolerate duplicate messages. Use of this mode can reduce session overhead by minimizing the work the session does to prevent duplicates.|
|SESSION_TRANSACTED|Session is Transacted and the acknowledge of messages is handled internally.|
|INDIVIDUAL_ACKNOWLEDGE|Acknowledges are applied to a single message only.  Unlike CLIENT_ACKNOWLEDGE where the acknowledgement applies to all messages received up to that point for the entire session, this mode applied only to a single message allowing the client to be more selective about which messages are acknowledged.|

In the previous section we showed you how to create a session, lets take a look at that example again here:

```
std::auto_ptr<cms::ConnectionFactory> connectionFactory( 
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );

std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );

std::auto_ptr<cms::Session> session( connection->createSession() );
```

In this code snippet the Session is created using the createSession with no arguments, this creates a Session that is in the AUTO_ACKNOWLEDGE mode. To create a Session with one of the modes listed above there is a second create method in the CMS Session interface that takes a single argument specifying the mode, lets take a look at an example:

```
std::auto_ptr<cms::ConnectionFactory> connectionFactory( 
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );

std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );

std::auto_ptr<cms::Session> session( connection->createSession( cms::Session::CLIENT_ACKNOWLEDGE ) );
```

As you can see there's not much difference here, just specify the mode you want for acknowledgement and you are ready to create your Session resources, in the next few section we will walk through the types of objects that you can create from a Session and show you the basics of using them.

## Objects created from CMS Session

In this section we cover the types of objects that are created from an instance of a CMS Session object.

### CMS Messages

As its name implies CMS exists to send and receive messages, so it makes sense to start by covering the message that you can send and receive using CMS. There are four main Message types supported by CMS at the the time of this writing, others may follow but we will stick to the ones that are fully supported right now. The table below shows the message type along with a brief description of the Message's use, for complete details on the interface and usage of a particular message type, see the [CMS API Documentation](/components/cms/documentation#api).


|Message Type|Description|
|---|---|
|Message|The Message interface defines the simplest CMS Message. Unlike the rest of the CMS Messages the Message type has no body or payload, it can however contains properties set via a collection of property setters that encompass the basic types in C++. The Message interface is the root of all the Message types in CMS.|
|TextMessage|The TextMessage class carries a payload consisting of a C++ string. The TextMessage interface extends the Message interface adding methods for setting and getting the payload text and retains the support for setting Message properties. Since Java Objects can't be sent to a CMS client from a JMS client directly the Text message is an ideal way to serialize the object to XML and send it to a JMS client.|
|BytesMessage|The BytesMessage payload consists of a series of uninterrupted bytes which the receiver is responsible for interpreting. The BytesMessage adds methods for getting and setting byte array to the standard set of Message interface methods.|
|MapMessage|The Map message payload is a set of Name / Value pairs. The Name is a C++ string type and the value is a C++ primitive type or string.|
|StreamMessage|The Stream Message body consists of a list of self describing primitive types.  The StreamMessage interface provides accessor methods that can read and write the primitive types to/from the message.  The read methods allow primitive type conversion when the conversion would result in no loss of data.|

Now that we've seen the types of Message's we can create lets see how to actually create them and explore some of the operations that are available in the Message classes.

### Creating a Message

As you may have already guessed, Messages are created using the CMS Session instance we created previously. The Session supplies methods for creating each of the four Message types we covered above. The Session is the Factory that create the providers implementation of the Message interfaces defined in CMS, it knows how to configure the internal data structures and prevents the client from being tied to the provider implementation directly, this is why we have to use the Session to create a Message object instead of creating them directly. Lets take a look at a code snippet that create a TextMessage instance and sets some properties on that Message.

```
// Create the ConnectionFactory
std::auto_ptr<cms::ConnectionFactory> connectionFactory(
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );

// Create a Connection
std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );

// Create a new Session from our Connection
std::auto_ptr<cms::Session> session( connection->createSession() );

// Now create a TextMessage
std::auto_ptr<cms::TextMessage> textMessage( session->createTextMessage() );

// Set the payload
textMessage->setText( "Payload Text" );

// Set some Properties
textMessage->setStringProperty( "USER_NAME", "Steve" );
textMessage->setIntProperty( "USER_CODE", 42 );
```

As you can see from the code above creating a TextMessage is much like creating a Session or Connection instance, you just call the **createTextMessage** in your instance of a CMS Session and you get back a new TextMessage pointer that you can then populate with text and properties.

### CMS Destinations

As its name implies the CMS Destination interface defines an object that represents an endpoint that the Messages are routed to by the Messaging broker. Clients create Destinations and send messages to them or wait for messages to be received on a destination they have subscribed to. There are two basic types of Destinations in CMS, the Topic and the Queue, there are two subtypes of these the Temporary Topic and Temporary Queue. The table below summarizes the four different Destination types.

|Destination Type|Description|
|---|---|
|Topic|In CMS a Topic implements publish and subscribe semantics. When you publish a message it goes to all the subscribers who are interested - so zero to many subscribers will receive a copy of the message. Only subscribers who had an active subscription at the time the broker receives the message will get a copy of the message.|
|Queue|A CMS Queue implements load balancer semantics. A single message will be received by exactly one consumer. If there are no consumers available at the time the message is sent it will be kept until a consumer is available that can process the message. If a consumer receives a message and does not acknowledge it before closing then the message will be redelivered to another consumer. A queue can have many consumers with messages load balanced across the available consumers.|
|TemporaryTopic|A TemporaryTopic object is a unique Topic object created for the duration of a Connection. It is a system-defined topic that can be consumed only by the Connection that created it.|
|TemporaryQueue|A TemporaryQueue object is a unique Queue object created for the duration of a Connection. It is a system-defined queue that can be consumed only by the Connection that created it.|

Now that we've seen what the destination types are, lets look at a code snippet showing how to create a Destination object. The example below shows how to create a Topic instance using what should now be a pretty familiar pattern.

```
// Create the ConnectionFactory
std::auto_ptr<cms::ConnectionFactory> connectionFactory(
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );

// Create a Connection
std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );

// Create a new Session from our Connection
std::auto_ptr<cms::Session> session( connection->createSession() );

// Now create a Topic
std::auto_ptr<cms::Topic> myTopic( session->createTopic( "EXAMPLE-TOPIC" ) );
```

Creating a Topic or Queue requires passing a name for the Destination, the name is similar to an address, messages sent the the "EXAMPLE-TOPIC" destination are received by clients that have subscribed to that same Destination.

### CMS MessageConsumer

Now that we've covered how to create messages and destinations we will look at creating a CMS MessageConsumer. The MessageConsumer allows the client application to receive messages sent by other clients to topics or queues. Before we talk about receiving Messages with the consumer lets first look at how one is created.

```
// Create the ConnectionFactory
std::auto_ptr<cms::ConnectionFactory> connectionFactory(
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );

// Create a Connection
std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );

// Create a new Session from our Connection
std::auto_ptr<cms::Session> session( connection->createSession() );

// Now create a Topic
std::auto_ptr<cms::Topic> myTopic( session->createTopic( "EXAMPLE-TOPIC" ) );

// Now create the Consumer
std::auto_ptr<cms::MessageConsumer> myConsumer( session->createConsumer( myTopic ) );
```

As you can see the MessageConsumer is created by calling **createConsumer** from an instance of a CMS Session object. The MessageConsumer is given a Destination to listen for Messages on at the time you create it. Once you create a MessageConsumer its time to start using it to receive messages, the MessageConsumer two methods for receiving messages, one is synchronous and the other is asynchronous.

The synchronous method of receiving Messages involves a call the to the consumers receive method. A call to receive will block until a Message is received on the Destination in question if no time-out is given, or will return NULL if the time-out given elapses and no new Messages arrive. Lets take a look at a simple example of a synchronous message polling loop using CMS.

```
while( !done ) {

   std::auto_ptr<Message> message( myConsumer->receive() );

   ...Do Something with the message...
}
```

As you can see in the code above we called the MessageConsumer's **receive** method expecting that it will return a new Message object at some point. The code here will block until a new message is received. This sort of approach assumes that your program has nothing else that it needs to do until a message arrives, however there is an alternative should your application need to perform other processing. The code sample below shows a polling loop that uses the MessageConsumer's **receiveNoWait** method to poll and return immediately to allow for other processing to occur.

```while( !done ) {

   std::auto_ptr<Message> message( myConsumer->receiveNoWait() );

   if( message.get() != NULL ) {
      ...Do Something with the message...
   }

   ...Perform other application logic before checking for another message...
}
```

The asynchronous method involves implementing the CMS MessageListener interface and passing an instance of your implementation to the MessageConsumer's **setMessageListener** method. When a new message arrives your listener's **onMessage** method will be called by the consumer in the context of another thread to allow you to process the Message received. Below is a code snippet that demonstrates implementing the MessageListener interface.

```
class SimpleListener : public cms::MessageListener {

    virtual void onMessage( const Message* message ) {

        const TextMessage* textMessage =
            dynamic_cast< const TextMessage* >( message );
        string text = "";

        if( textMessage != NULL ) {
            text = textMessage->getText();
        } else {
            text = "NOT A TEXTMESSAGE!";
        }

        printf( "Message Received: %s\n", text.c_str() );

    }
};
```

In the sample above we create a new class called SimpleListener that prints the contents of a TextMessage when received, or print an message indicating that it did not receive a TextMessage as it had expected to. Notice that the **onMessage** method receives a pointer to the base Message interface and we then attempt to dynamic cast to the type we think we should have received. This allows your code to process multiple message types in one method. The pointer passed is owned by the caller or onMessage so you shouldn't store it or delete it, if you need to keep a copy of the Message around you must create a copy by calling Message's **clone** method.

Now that we have a MessageListener implementation to work with its time to see how to setup asynchronous consumption using the MessageConsumer we created previously.

```
SimpleListener listener;

myConsumer->setMessageListener( &listener );
```

That's it, we will now receive the messages sent to the Destination we created in the onMessage method of our SimpleListener instance.

### CMS MessageProducer

We've seen how to consume Messages now how do we produce them in the first place? The answer is the CMS MessageProducer which is used to send messages to the broker for distribution to the various clients listening for Messages on the topic or queue. Creating a MessageProducer is much the same as creating a MessageConsumer, you first create your Connection, Session, and Destination objects then use the Session to create a MessageProducer. The code snippet below demonstrates how to create a MessageProducer.

```
// Create the ConnectionFactory
std::auto_ptr<cms::ConnectionFactory> connectionFactory(
    cms::ConnectionFactory::createCMSConnectionFactory( "tcp://127.0.0.1:61616" ) );

// Create a Connection
std::auto_ptr<cms::Connection> connection( connectionFactory->createConnection() );

// Create a new Session from our Connection
std::auto_ptr<cms::Session> session( connection->createSession() );

// Now create a Topic
std::auto_ptr<cms::Topic> myTopic( session->createTopic( "EXAMPLE-TOPIC" ) );

// Now create the Consumer
std::auto_ptr<cms::MessageProducer> myProducer( session->createProducer( myTopic ) );
```

## Complete Examples

Now that we've covered most of the basics of the CMS API its time to take a look at some complete examples that demonstrate how you might use the CMS API in your own application. The first example will show how to create a simple Asynchronous consumer that can receive TextMessage objects from an ActiveMQ broker, then in the second example we will look at a simple producer that publishes TextMessage objects to the destination that our consumer is listening on.

### Simple Asynchronous Consumer

In the Simple Asnychronous Consumer example we wrap the CMS API usage in a class called SimpleAsyncConsumer. This class exposes a single constructor that allows the user to create an instance of the class that connects to a particular broker and destination and whether the destination is a Queue or Topic. The user can also specify the Acknowledgement Mode shoukd be **CLIENT_ACKNOWLEDGE** instaned of the default **AUTO_ACKNOWLEDGE** mode.

Once an instance of this class is created the user calls the _runConsumer_ method to start listening on the specified destination. The _runConsumer_ method creates a Connection to the broker and start a new Session configured with the configured Acknowledgement mode. Once a Session is created a new Consumer can then be created and attached to the configued Destination. Since we want to listen asynchronously for new messages the SimpleAsyncConsumer inherits from cms::MessageListener so that it can register itself as a Message Listener with the MessageConsumer created in _runConsumer_.

After the _runConsumer_ method returns the main method waits for user input to exit, all messages received while the application is running will be dispatched to the onMessage method of SimpleAsyncConsumer and if the message is a TextMessage its contents will be printed onscreen.

```
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

#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/core/ActiveMQConnection.h>
#include <activemq/transport/DefaultTransportListener.h>
#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Integer.h>
#include <activemq/util/Config.h>
#include <decaf/util/Date.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>

using namespace activemq;
using namespace activemq::core;
using namespace activemq::transport;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
class SimpleAsyncConsumer : public ExceptionListener,
                            public MessageListener,
                            public DefaultTransportListener {
private:

    Connection* connection;
    Session* session;
    Destination* destination;
    MessageConsumer* consumer;
    bool useTopic;
    std::string brokerURI;
    std::string destURI;
    bool clientAck;

private:

    SimpleAsyncConsumer( const SimpleAsyncConsumer& );
    SimpleAsyncConsumer& operator= ( const SimpleAsyncConsumer& );

public:

    SimpleAsyncConsumer( const std::string& brokerURI,
                         const std::string& destURI,
                         bool useTopic = false,
                         bool clientAck = false ) :
        connection(NULL),
        session(NULL),
        destination(NULL),
        consumer(NULL),
        useTopic(useTopic),
        brokerURI(brokerURI),
        destURI(destURI),
        clientAck(clientAck) {
    }

    virtual ~SimpleAsyncConsumer() {
        this->cleanup();
    }

    void close() {
        this->cleanup();
    }

    void runConsumer() {

        try {

            // Create a ConnectionFactory
            ActiveMQConnectionFactory* connectionFactory =
                new ActiveMQConnectionFactory( brokerURI );

            // Create a Connection
            connection = connectionFactory->createConnection();
            delete connectionFactory;

            ActiveMQConnection* amqConnection = dynamic_cast<ActiveMQConnection*>( connection );
            if( amqConnection != NULL ) {
                amqConnection->addTransportListener( this );
            }

            connection->start();

            connection->setExceptionListener(this);

            // Create a Session
            if( clientAck ) {
                session = connection->createSession( Session::CLIENT_ACKNOWLEDGE );
            } else {
                session = connection->createSession( Session::AUTO_ACKNOWLEDGE );
            }

            // Create the destination (Topic or Queue)
            if( useTopic ) {
                destination = session->createTopic( destURI );
            } else {
                destination = session->createQueue( destURI );
            }

            // Create a MessageConsumer from the Session to the Topic or Queue
            consumer = session->createConsumer( destination );
            consumer->setMessageListener( this );

        } catch (CMSException& e) {
            e.printStackTrace();
        }
    }

    // Called from the consumer since this class is a registered MessageListener.
    virtual void onMessage( const Message* message ) {

        static int count = 0;

        try
        {
            count++;
            const TextMessage* textMessage =
                dynamic_cast< const TextMessage* >( message );
            string text = "";

            if( textMessage != NULL ) {
                text = textMessage->getText();
            } else {
                text = "NOT A TEXTMESSAGE!";
            }

            if( clientAck ) {
                message->acknowledge();
            }

            printf( "Message #%d Received: %s\n", count, text.c_str() );
        } catch (CMSException& e) {
            e.printStackTrace();
        }
    }

    // If something bad happens you see it here as this class is also been
    // registered as an ExceptionListener with the connection.
    virtual void onException( const CMSException& ex AMQCPP_UNUSED ) {
        printf("CMS Exception occurred.  Shutting down client.\n");
        exit(1);
    }

    virtual void transportInterrupted() {
        std::cout << "The Connection's Transport has been Interrupted." << std::endl;
    }

    virtual void transportResumed() {
        std::cout << "The Connection's Transport has been Restored." << std::endl;
    }

private:

    void cleanup(){

        try {
            if( connection != NULL ) {
                connection->close();
            }
        } catch ( CMSException& e ) { 
            e.printStackTrace(); 
        }

        delete destination;
        delete consumer;
        delete session;
        delete connection;
    }
};

////////////////////////////////////////////////////////////////////////////////
int main(int argc AMQCPP_UNUSED, char* argv[] AMQCPP_UNUSED) {

    activemq::library::ActiveMQCPP::initializeLibrary();

    std::cout << "=====================================================\n";
    std::cout << "Starting the example:" << std::endl;
    std::cout << "-----------------------------------------------------\n";

    // Set the URI to point to the IPAddress of your broker.
    // add any optional params to the url to enable things like
    // tightMarshalling or tcp logging etc.  See the CMS web site for
    // a full list of configuration options.
    //
    //  http://activemq.apache.org/cms/
    //
    std::string brokerURI =
        "failover:(tcp://127.0.0.1:61616)";

    //============================================================
    // This is the Destination Name and URI options.  Use this to
    // customize where the consumer listens, to have the consumer
    // use a topic or queue set the 'useTopics' flag.
    //============================================================
    std::string destURI = "TEST.FOO"; //?consumer.prefetchSize=1";

    //============================================================
    // set to true to use topics instead of queues
    // Note in the code above that this causes createTopic or
    // createQueue to be used in the consumer.
    //============================================================
    bool useTopics = false;

    //============================================================
    // set to true if you want the consumer to use client ack mode
    // instead of the default auto ack mode.
    //============================================================
    bool clientAck = false;

    // Create the consumer
    SimpleAsyncConsumer consumer( brokerURI, destURI, useTopics, clientAck );

    // Start it up and it will listen forever.
    consumer.runConsumer();

    // Wait to exit.
    std::cout << "Press 'q' to quit" << std::endl;
    while( std::cin.get() != 'q') {}

    // All CMS resources should be closed before the library is shutdown.
    consumer.close();

    std::cout << "-----------------------------------------------------\n";
    std::cout << "Finished with the example." << std::endl;
    std::cout << "=====================================================\n";

    activemq::library::ActiveMQCPP::shutdownLibrary();
}
```

### Simple Producer

Much like the Simple Asynchronous Consumer example the Simple Producer example wraps up the CMS API detials needed to create a producer into a class called **SimpleProducer**. This class exposes a simillar interface to the consumer example, there is one constructor that allows an instance to be created with configuration options for the broker and destination as well as the number of messages to send to the configured destination. Once created the client code simply calls the SimpleProducer's _run_ method to publish the specified number of messages. Once the _run_ method completes the client is free to close the SimpleProducer which cleans up the allocated CMS resources, once closed the application exits.

A Simple Message Producer Example

```
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

#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>
#include <decaf/lang/Long.h>
#include <decaf/util/Date.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/util/Config.h>
#include <activemq/library/ActiveMQCPP.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>

using namespace activemq;
using namespace activemq::core;
using namespace decaf;
using namespace decaf::lang;
using namespace decaf::util;
using namespace decaf::util::concurrent;
using namespace cms;
using namespace std;

////////////////////////////////////////////////////////////////////////////////
class SimpleProducer : public Runnable {
private:

    Connection* connection;
    Session* session;
    Destination* destination;
    MessageProducer* producer;
    bool useTopic;
    bool clientAck;
    unsigned int numMessages;
    std::string brokerURI;
    std::string destURI;

private:

    SimpleProducer( const SimpleProducer& );
    SimpleProducer& operator= ( const SimpleProducer& );

public:

    SimpleProducer( const std::string& brokerURI, unsigned int numMessages,
                    const std::string& destURI, bool useTopic = false, bool clientAck = false ) :
        connection(NULL),
        session(NULL),
        destination(NULL),
        producer(NULL),
        useTopic(useTopic),
        clientAck(clientAck),
        numMessages(numMessages),
        brokerURI(brokerURI),
        destURI(destURI) {
    }

    virtual ~SimpleProducer(){
        cleanup();
    }

    void close() {
        this->cleanup();
    }

    virtual void run() {
        try {

            // Create a ConnectionFactory
            auto_ptr<ActiveMQConnectionFactory> connectionFactory(
                new ActiveMQConnectionFactory( brokerURI ) );

            // Create a Connection
            try{
                connection = connectionFactory->createConnection();
                connection->start();
            } catch( CMSException& e ) {
                e.printStackTrace();
                throw e;
            }

            // Create a Session
            if( clientAck ) {
                session = connection->createSession( Session::CLIENT_ACKNOWLEDGE );
            } else {
                session = connection->createSession( Session::AUTO_ACKNOWLEDGE );
            }

            // Create the destination (Topic or Queue)
            if( useTopic ) {
                destination = session->createTopic( destURI );
            } else {
                destination = session->createQueue( destURI );
            }

            // Create a MessageProducer from the Session to the Topic or Queue
            producer = session->createProducer( destination );
            producer->setDeliveryMode( DeliveryMode::NON_PERSISTENT );

            // Create the Thread Id String
            string threadIdStr = Long::toString( Thread::currentThread()->getId() );

            // Create a messages
            string text = (string)"Hello world! from thread " + threadIdStr;

            for( unsigned int ix=0; ix<numMessages; ++ix ){
                TextMessage* message = session->createTextMessage( text );

                message->setIntProperty( "Integer", ix );

                // Tell the producer to send the message
                printf( "Sent message #%d from thread %s\n", ix+1, threadIdStr.c_str() );
                producer->send( message );

                delete message;
            }

        }catch ( CMSException& e ) {
            e.printStackTrace();
        }
    }

private:

    void cleanup(){

        try {
            if( connection != NULL ) {
                connection->close();
            }
        } catch ( CMSException& e ) { 
            e.printStackTrace(); 
        }

        delete destination;
        delete producer;
        delete session;
        delete connection;
    }
};

////////////////////////////////////////////////////////////////////////////////
int main(int argc AMQCPP_UNUSED, char* argv[] AMQCPP_UNUSED) {

    activemq::library::ActiveMQCPP::initializeLibrary();

    std::cout << "=====================================================\n";
    std::cout << "Starting the example:" << std::endl;
    std::cout << "-----------------------------------------------------\n";

    // Set the URI to point to the IPAddress of your broker.
    // add any optional params to the url to enable things like
    // tightMarshalling or tcp logging etc.  See the CMS web site for
    // a full list of configuration options.
    //
    //  http://activemq.apache.org/cms/
    //
    std::string brokerURI =
        "failover://(tcp://127.0.0.1:61616)";

    //============================================================
    // Total number of messages for this producer to send.
    //============================================================
    unsigned int numMessages = 2000;

    //============================================================
    // This is the Destination Name and URI options.  Use this to
    // customize where the Producer produces, to have the producer
    // use a topic or queue set the 'useTopics' flag.
    //============================================================
    std::string destURI = "TEST.FOO";

    //============================================================
    // set to true to use topics instead of queues
    // Note in the code above that this causes createTopic or
    // createQueue to be used in the producer.
    //============================================================
    bool useTopics = false;

    // Create the producer and run it.
    SimpleProducer producer( brokerURI, numMessages, destURI, useTopics );

    // Publish the given number of Messages
    producer.run();

    // Before exiting we ensure that all CMS resources are closed.
    producer.close();

    std::cout << "-----------------------------------------------------\n";
    std::cout << "Finished with the example." << std::endl;
    std::cout << "=====================================================\n";

    activemq::library::ActiveMQCPP::shutdownLibrary();
}
```
