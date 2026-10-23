---
title: "Handling Advisory Messages"
sidebar_position: 1
---
ActiveMQ supports advisory messages which allows you to watch the system using regular CMS messages. A few things that you can currently do with advisory messages are:

*   See consumers, producers and connections starting and stopping
*   See temporary destinations being created and destroyed
*   Get notified messages expiring on topics and queues
*   Observe brokers sending messages to destinations with no consumers.
*   See connections starting and stopping

Advisory messages can be thought as some kind of administrative channel where you receive information regarding what is happening on your JMS provider along with what's happening with producers, consumers and destinations. To learn more about the broker's advisory message support see this [article](/components/classic/documentation/advisory-message).

### **Before You Begin**

This tutorial assumes that the reader has a working knowledge of the CMS API and knows how to build basic applications using the ActiveMQ-CPP Library. If you aren't sure how to use the CMS API then you should first read the [CMS API Overview](/components/cms/overview). The tutorial is written against the ActiveMQ-CPP 3.0 release API and while it is possible to handle advisory messages using the older ActiveMQ-CPP 2.x clients there may be some code differences which are not shown in this article.

### **Subscribing to Advisory Topics**

To receive advisory message's you first need to subscribe to the Topic that supplies the advisory message you want. You subscribe to these Topics like you would any other destination, the trick is to use the correct name for the Topic that you want to use. Lets first take a look at a couple of the types available for use (this is not a complete set).

|Advisory Topics|Description|properties|Data Structure|
|---|---|---|---|
|ActiveMQ.Advisory.Connection|Connection start & stop messages|||
|ActiveMQ.Advisory.Producer.Queue|Producer start & stop messages on a Queue|String='producerCount' - the number of producers|ProducerInfo|
|ActiveMQ.Advisory.Producer.Topic|Producer start & stop messages on a Topic|String='producerCount' - the number of producers|ProducerInfo|
|ActiveMQ.Advisory.Consumer.Queue|Consumer start & stop messages on a Queue|String='consumerCount' - the number of Consumers|ConsumerInfo|
|ActiveMQ.Advisory.Consumer.Topic|Consumer start & stop messages on a Topic|String='consumerCount' - the number of Consumers|ConsumerInfo|

Now, looking at the above list lets pick one of the Topics and figure out how we would create the CMS Topic to subscribe to in order to receive the advisory messages. Using the Java client we could create the Destination using the utility class **AdvisorySupport** but for now ActiveMQ-CPP doesn't provide such a utility class, hopefully we will see this added in a future release (Contributions are of course always welcomed!).

If we had a Topic named TOPIC.FOO and we wanted to know when a producer subscribed to that Topic we would need to create a Topic object whose name is **ActiveMQ.Advisory.Producer.Topic.TOPIC.FOO** in order to receive the advisory message we are interested in. We know this because we can look at the above table and see that the **ActiveMQ.Advisory.Producer.Topic** is informed whenever a Producer starts or stops publishing messages on a Topic and we also know that our Topic is named TOPIC.FOO, so adding them together gets us the name of our Advisory Topic, we also know this because we peaked at the AdvisorySupport.java class, and no, that's not cheating. Below is a code snippet showing the creation of the Topic using a CMS Session:

```
std::auto_ptr<cms::Topic> advisories( session->createTopic(
    "ActiveMQ.Advisory.Producer.Topic.TOPIC.FOO" ) );
```
Once we create the Topic for the advisory messages we want to listen for we just need to create a consumer to listen for them, the code snippet below demonstrates this:

```
std::auto_ptr<cms::MessageConsumer> consumer;
consumer.reset( session->createConsumer( advisories.get() ) );
consumer->setMessageListener( this );
```
As you can see there is no difference between subscriptions to advisory topics that subscriptions to any other Destination in CMS. In the above example we registered as an asynchronous listener, you can also use the normal blocking **receive** method but we prefer this method.

### **Dealing with incoming Advisory Messages**

Every Advisory has the message type 'Advisory' and some predefined message properties, to check a CMS Message for this type you would call the **getCMSType** method on a Message object. In some cases you will know that the only message you are going to receive are advisory messages because your client only subscribes to advisory topics, other times you might have wired multiple MessageConsumers to the same MessageListener in which case you will have to check the message tppe. Once you know you are dealing with an advisory message then you can start to inspect it to determine what sort of message it is and extract the data that is meaningful to your application.

Many of the advisory messages store meaningful data in the Message properties, for instance the Consumer Start / Stop advisory message contains an element with the key **consumerCount** which is populated with the current number of active consumers on the Topic or Queue in question. Lets take a look at a code snippet now that checks the messages received in an onMessage callback to see if its an advisory message and acts on it if it is:

```
void AdvisoryProducer::onMessage( const cms::Message* message ) {

   if( message->getCMSType() == "Advisory" ) {

       std::cout << "Received an Advisory Message!" << std::endl;

       if( message->propertyExists( "consumerCount" ) ) {

           std::string consumerCount = message->getStringProperty( "consumerCount" );
           std::cout << "Number of Consumers = " << consumerCount << std::endl;

           // Do Something Meaningful here....
       }

   } else {
       std::cout << "Received a Non-Advisory Message!" << std::endl;
   }
}
```
### Complete Example: Producer that only Produces when there is a Consumer

Now that you've seen the basics of advisory message processing its time to show you a complete example that demonstrates what you can do with advisory messages. The following code shows a class header and source file that implements a basic CMS Producer that will send heart beat message to a Topic called **HEART-BEAT-CHANNEL** only when there are active consumers, otherwise it sits idle.

```
#ifndef _ACTIVEMQCPP_EXAMPLES_ADVISORIES_ADVISORYPRODUCER_H_
#define _ACTIVEMQCPP_EXAMPLES_ADVISORIES_ADVISORYPRODUCER_H_

#include <string>
#include <memory>

#include <cms/Session.h>
#include <cms/MessageProducer.h>
#include <cms/MessageConsumer.h>
#include <cms/MessageListener.h>

#include <decaf/lang/Runnable.h>
#include <decaf/util/concurrent/CountDownLatch.h>

namespace activemqcpp {
namespace examples {
namespace advisories {

    /**
     * A sample Producer that will only send Messages on its Topic when it has
     * received an advisory indicating that there is an active MessageConsumer
     * on the Topic.  Once another message comes in indicating that there is no
     * longer a consumer then this Producer stops producing again.
     *
     * @since 3.0
     */
    class AdvisoryProducer : public decaf::lang::Runnable,
                             public cms::MessageListener {
    private:

        volatile bool consumerOnline;
        volatile bool shutdown;
        decaf::util::concurrent::CountDownLatch shutdownLatch;

        cms::Session* session;
        std::auto_ptr<cms::MessageConsumer> consumer;
        std::auto_ptr<cms::MessageProducer> producer;

    public:

        AdvisoryProducer( cms::Session* session );
        virtual ~AdvisoryProducer();

        /**
         * Shut down the processing that occurs in the Run method.
         */
        void stop();

        /**
         * Run the producer code.
         */
        virtual void run();

        /**
         * Async Message callback.
         */
        virtual void onMessage( const cms::Message* message );

    };

}}}

#endif /* _ACTIVEMQCPP_EXAMPLES_ADVISORIES_ADVISORYPRODUCER_H_ */
```
**AdvisoryProducer Source file**
```
#include "AdvisoryProducer.h"

#include <cms/Topic.h>
#include <cms/Message.h>
#include <cms/TextMessage.h>
#include <decaf/lang/exceptions/NullPointerException.h>
#include <decaf/lang/Integer.h>

using namespace std;
using namespace activemqcpp;
using namespace activemqcpp::examples;
using namespace activemqcpp::examples::advisories;
using namespace decaf;
using namespace decaf::lang;
using namespace decaf::lang::exceptions;

////////////////////////////////////////////////////////////////////////////////
AdvisoryProducer::AdvisoryProducer( cms::Session* session ) : shutdownLatch(1) {

    if( session == NULL ) {
        throw NullPointerException(
            __FILE__, __LINE__, "Session Object passed was Null." );
    }

    std::auto_ptr<cms::Topic> destination( session->createTopic(
        "HEART-BEAT-CHANNEL" ) );
    std::auto_ptr<cms::Topic> advisories( session->createTopic(
        "ActiveMQ.Advisory.Consumer.Topic.HEART-BEAT-CHANNEL" ) );

    this->shutdown = false;
    this->consumerOnline = false;

    this->session = session;
    this->producer.reset( session->createProducer( destination.get() ) );
    this->consumer.reset( session->createConsumer( advisories.get() ) );
    this->consumer->setMessageListener( this );
}

////////////////////////////////////////////////////////////////////////////////
AdvisoryProducer::~AdvisoryProducer() {
}

////////////////////////////////////////////////////////////////////////////////
void AdvisoryProducer::stop() {
    this->shutdown = true;
    this->shutdownLatch.await( 3000 );
}

////////////////////////////////////////////////////////////////////////////////
void AdvisoryProducer::run() {

    while( !this->shutdown ) {

        if( this->consumerOnline ) {

            std::auto_ptr<cms::TextMessage> message(
                this->session->createTextMessage( "Alive" ) );

            this->producer->send( message.get() );

            Thread::sleep( 1000 );
        }
    }

    this->shutdownLatch.countDown();
}

////////////////////////////////////////////////////////////////////////////////
void AdvisoryProducer::onMessage( const cms::Message* message ) {

    if( message->getCMSType() == "Advisory" ) {

        std::cout << "Received an Advisory Message!" << std::endl;

        if( message->propertyExists( "consumerCount" ) ) {

            std::string consumerCount = message->getStringProperty( "consumerCount" );
            std::cout << "Number of Consumers = " << consumerCount << std::endl;
            this->consumerOnline = Integer::parseInt( consumerCount ) > 0 ? true : false;
        }

    } else {
        std::cout << "Received a Non-Advisory Message!" << std::endl;
    }
}
```
### ***Advanced Topic*** Dealing with Command Objects in Advisories

If you read the ActiveMQ Advisory Message [article](/components/classic/documentation/advisory-message) mentioned at the start of this tutorial then you know that certain advisory messages can contain an embedded command object. If you didn't read that [article](/components/classic/documentation/advisory-message) then this section is going to be very confusing, so go read it. We can access those command object in CMS with a little work which means we can take full advantage of the Advisory Message feature.

All Advisory Messages are sent as a basic ActiveMQMessage to your client. The underlying type hierarchy in ActiveMQ-CPP is the same as that of ActiveMQ so the names of the embedded command objects you saw in the Advisory [article](/components/classic/documentation/advisory-message) are the same and they contain mostly the same information, although sometimes the information is encoded in a more C++ friendly or unfriendly way depending on your point of view.

To demonstrate how we can access the command objects lets try and create a client application that listens to the Broker for advisories that indicate that Temporary Destinations have either been created or destroyed. The Broker will publish advisory messages to the "ActiveMQ.Advisory.TempTopic" and "ActiveMQ.Advisory.TempQueue" Topics whenever the corresponding Temporary Destination is created or destroyed and the command object will be of type DestinationInfo. The DestinationInfo object contains a Destination object describing the Destination in question and an Operation Type value telling whether the command is a create or destroy command. First lets look at how we subscribe to this Advisory Topic:

**Subscribing to a Composite Advisory Topic**
```
std::auto_ptr<cms::Topic> advisories( session->createTopic(
    "ActiveMQ.Advisory.TempTopic,ActiveMQ.Advisory.TempQueue" ) );

std::auto_ptr<cms::MessageConsumer> consumer;
consumer.reset( session->createConsumer( advisories.get() ) );
consumer->setMessageListener( this );
```
As you can see in the above code snippet we just create a new Topic object whose name is a composite of the two Topics we want to subscribe on, this will allow our single **MessageConsumer** instance to receive both Temporary Topic and Temporary Queue advisories. As before we also create a **MessageConsumer** and register our class' instance as the asynchronous listener. Now all that's left is to implement the **onMessage** method of the **MessageListener** interface, lets take a look at that code now:

**Processing an Advisory message with an embedded command object**
```
////////////////////////////////////////////////////////////////////////////////
void TempDestinationAdvisoryConsumer::onMessage( const cms::Message* message ) {

    if( message->getCMSType() == "Advisory" ) {

        std::cout << "Received an Advisory Message!" << std::endl;

        const ActiveMQMessage* amqMessage =
            dynamic_cast<const ActiveMQMessage*>( message );

        if( amqMessage != NULL && amqMessage->getDataStructure() != NULL ) {
            std::cout << "Advisory Message contains a Command Object!" << std::endl;

            try {

                Pointer<DestinationInfo> info =
                    amqMessage->getDataStructure().dynamicCast<DestinationInfo>();

                unsigned char operationType = info->getOperationType();

                if( operationType == ActiveMQConstants::DESTINATION_REMOVE_OPERATION ) {
                    std::cout << "Temporary Destination {"
                              << info->getDestination()->getPhysicalName()
                              << "} Removed."
                              << std::endl;
                } else if( operationType == ActiveMQConstants::DESTINATION_ADD_OPERATION ) {
                    std::cout << "Temporary Destination {"
                              << info->getDestination()->getPhysicalName()
                              << "} Added."
                              << std::endl;
                } else {
                    std::cout << "ERROR: I have no Idea what just happened!"
                              << std::endl;
                }

            } catch( ClassCastException& ex ) {
                std::cout << "ERROR: Expected the Command to be a DestinationInfo, "
                          << "it wasn't so PANIC!!"
                          << std::endl;
            }
        }

    } else {
        std::cout << "Received a Non-Advisory Message!" << std::endl;
    }
}
```
Fortunately for use the code above looks more complicated than it really is, lets walk through it a bit more slowly now to understand what is going on:

**Getting to the ActiveMQMessage object**
```
if( message->getCMSType() == "Advisory" ) {

    std::cout << "Received an Advisory Message!" << std::endl;

    const ActiveMQMessage* amqMessage =
        dynamic_cast<const ActiveMQMessage*>( message );

    ... Other scary code comes next...

else {
    std::cout << "Received a Non-Advisory Message!" << std::endl;
}
```
The first thing we need to do is check that we received an advisory message, ActiveMQ encodes the Message Type as "Advisory" so that's easy enough. We don't technically need to do this here since our consumer only listens on an advisory Topic but its not a bad idea to check. Once we know its an advisory message we know that the message pointer should be of type ActiveMQMessage under that generic cms::Message disguise its wearing so we use a **dynamic_cast** to convert it. Now that we've converted to an ActiveMQMessage what's next, well lets take a look:

**Checking for an embedded command object**
```
if( amqMessage != NULL && amqMessage->getDataStructure() != NULL ) {
    std::cout << "Advisory Message contains a Command Object!" << std::endl;
```
Every ActiveMQMessage derived object has a method called **getDataStructure** which can be used for all sorts of useful things, here we are trying to see if there is a command object contained in this message, and you guessed it, the getDataStructure method will tell us if there is one. If there is then we can move onto checking for a DestinationInfo object:

**Getting to the DestinationInfo object**
```
try {

    Pointer<DestinationInfo> info =
        amqMessage->getDataStructure().dynamicCast<DestinationInfo>();

    unsigned char operationType = info->getOperationType();

    if( operationType == ActiveMQConstants::DESTINATION_REMOVE_OPERATION ) {
        std::cout << "Temporary Destination {"
                  << info->getDestination()->getPhysicalName()
                  << "} Removed."
                  << std::endl;
    } else if( operationType == ActiveMQConstants::DESTINATION_ADD_OPERATION ) {
        std::cout << "Temporary Destination {"
                  << info->getDestination()->getPhysicalName()
                  << "} Added."
                  << std::endl;
     } else {
        std::cout << "ERROR: I have no Idea what just happened!"
                  << std::endl;
     }

} catch( ClassCastException& ex ) {
    std::cout << "ERROR: Expected the Command to be a DestinationInfo, "
              << "it wasn't so PANIC!!"
              << std::endl;
}
```
First thing you are probably asking about this code snippet is "what is that Pointer thing?", that is a thread safe smart pointer that is used internally by ActiveMQ-CPP to manage all the pointers that make up the cms::Message objects among other things. We create an instance of a Pointer<DestinationInfo> type which will pointer to our DestinationInfo command if the dynamicCast method is able to make that conversion, if not a ClassCastException is thrown. Once we have the DestinationInfo pointer we can retrieve the Operation Type of the command and then compare it to the constants in ActiveMQConstants to see what is being done to the Destination. There are only two operation types, add and remove, but since the DestinationInfo object encodes the operation type value as an unsigned char we provide a fall-back case to alert us to that error. We are almost done now, all that remains is to output what happened, and also let the user know what the name of the Destination is, the **getPhysicalName** method in the Destination class tells us that. You could also find out whether the Destination is a Topic or a Queue using the Destination object, we leave that as an exercise to the reader.

### **Complete Example: Consumer that listens for Creation and Destruction of Temporary Destinations**

The complete code of our client application is shown below, you can also find this code as well as a simple client that creates both a Temporary Topic and a Temporary Queue in the examples folder in the source distribution.

**TempDestinationAdvisoryConsumer Header File**
```
#ifndef _ACTIVEMQCPP_EXAMPLES_ADVISORIES_TEMPDESTINATIONADVISORYCONSUMER_H_
#define _ACTIVEMQCPP_EXAMPLES_ADVISORIES_TEMPDESTINATIONADVISORYCONSUMER_H_

#include <string>
#include <memory>

#include <cms/Session.h>
#include <cms/MessageProducer.h>
#include <cms/MessageConsumer.h>
#include <cms/MessageListener.h>

#include <decaf/lang/Runnable.h>

namespace activemqcpp {
namespace examples {
namespace advisories {

    /**
     * Monitors a Broker for Temporary Topic creation and destruction.
     *
     * @since 3.0
     */
    class TempDestinationAdvisoryConsumer : public cms::MessageListener {
    private:

        cms::Session* session;
        std::auto_ptr<cms::MessageConsumer> consumer;

    public:

        TempDestinationAdvisoryConsumer( cms::Session* session );
        virtual ~TempDestinationAdvisoryConsumer();

        /**
         * Async Message callback.
         */
        virtual void onMessage( const cms::Message* message );

    };

}}}

#endif /* _ACTIVEMQCPP_EXAMPLES_ADVISORIES_TEMPDESTINATIONADVISORYCONSUMER_H_ */
```
**TempDestinationAdvisoryConsumer Source File**
```
#include "TempDestinationAdvisoryConsumer.h"

#include <cms/Topic.h>
#include <cms/Message.h>
#include <cms/TextMessage.h>
#include <activemq/core/ActiveMQConstants.h>
#include <activemq/commands/ActiveMQMessage.h>
#include <activemq/commands/DestinationInfo.h>
#include <decaf/lang/exceptions/NullPointerException.h>
#include <decaf/lang/exceptions/ClassCastException.h>
#include <decaf/lang/Integer.h>

using namespace std;
using namespace activemqcpp;
using namespace activemqcpp::examples;
using namespace activemqcpp::examples::advisories;
using namespace activemq;
using namespace activemq::commands;
using namespace activemq::core;
using namespace decaf;
using namespace decaf::lang;
using namespace decaf::lang::exceptions;

////////////////////////////////////////////////////////////////////////////////
TempDestinationAdvisoryConsumer::TempDestinationAdvisoryConsumer( cms::Session* session ) {

    if( session == NULL ) {
        throw NullPointerException(
            __FILE__, __LINE__, "Session Object passed was Null." );
    }

    std::auto_ptr<cms::Topic> advisories( session->createTopic(
        "ActiveMQ.Advisory.TempTopic,ActiveMQ.Advisory.TempQueue" ) );

    this->session = session;
    this->consumer.reset( session->createConsumer( advisories.get() ) );
    this->consumer->setMessageListener( this );
}

////////////////////////////////////////////////////////////////////////////////
TempDestinationAdvisoryConsumer::~TempDestinationAdvisoryConsumer() {
}

////////////////////////////////////////////////////////////////////////////////
void TempDestinationAdvisoryConsumer::onMessage( const cms::Message* message ) {

    if( message->getCMSType() == "Advisory" ) {

        std::cout << "Received an Advisory Message!" << std::endl;

        const ActiveMQMessage* amqMessage =
            dynamic_cast<const ActiveMQMessage*>( message );

        if( amqMessage != NULL && amqMessage->getDataStructure() != NULL ) {
            std::cout << "Advisory Message contains a Command Object!" << std::endl;

            try {

                Pointer<DestinationInfo> info =
                    amqMessage->getDataStructure().dynamicCast<DestinationInfo>();

                unsigned char operationType = info->getOperationType();

                if( operationType == ActiveMQConstants::DESTINATION_REMOVE_OPERATION ) {
                    std::cout << "Temporary Destination {"
                              << info->getDestination()->getPhysicalName()
                              << "} Removed."
                              << std::endl;
                } else if( operationType == ActiveMQConstants::DESTINATION_ADD_OPERATION ) {
                    std::cout << "Temporary Destination {"
                              << info->getDestination()->getPhysicalName()
                              << "} Added."
                              << std::endl;
                } else {
                    std::cout << "ERROR: I have no Idea what just happened!"
                              << std::endl;
                }

            } catch( ClassCastException& ex ) {
                std::cout << "ERROR: Expected the Command to be a DestinationInfo, "
                          << "it wasn't so PANIC!!"
                          << std::endl;
            }
        }

    } else {
        std::cout << "Received a Non-Advisory Message!" << std::endl;
    }
}
```
