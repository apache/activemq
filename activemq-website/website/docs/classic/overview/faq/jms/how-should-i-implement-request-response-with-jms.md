---
title: "How should I implement request response with JMS"
slug: /how-should-i-implement-request-response-with-jms
---

How should I implement request response with JMS?
-------------------------------------------------

The simplest solution is to use [Camel as a Spring Remoting provider](http://activemq.apache.org/camel/spring-remoting.html) which allows you to hide all the JMS API from your business logic and letting Camel provide the request/response handling code for you.

However if you wish to write the JMS client code yourself, please read on how it works...

### Using the JMS API to implement request-response

You might think at first that to implement request-response type operations in JMS that you should create a new consumer with a selector per request; or maybe create a new temporary queue per request.

Creating temporary destinations, consumers, producers and connections are all synchronous request-response operations with the broker and so should be avoided for processing each request as it results in lots of chat with the JMS broker.

The best way to implement request-response over JMS is to create a temporary queue and consumer per client on startup, set JMSReplyTo property on each message to the temporary queue and then use a [correlationID on each message](http://java.sun.com/j2ee/1.4/docs/api/javax/jms/Message.html#setJMSCorrelationID(java.lang.String)) to correlate request messages to response messages. This avoids the overhead of creating and closing a consumer for each request (which is expensive). It also means you can share the same producer & consumer across many threads if you want (or pool them maybe).

The [Lingo library](http://lingo.codehaus.org/) is an implementation of Spring remoting using JMS. (Spring remoting is a kind of POJO based remoting where the remoting code is invisible to your business logic code).

It uses exactly this pattern; of using correlation IDs to correlate requests to responses. The server side just has to remember to put the inbound message's correlation ID on the response.

The actual class which does this is the [MultiplexingRequestor](http://lingo.codehaus.org/maven/apidocs/org/logicblaze/lingo/jms/impl/MultiplexingRequestor.html) . It may be just using Spring remoting with Lingo is the simplest way of implementing request response - or maybe you could just use Lingo's [Requestor](http://lingo.codehaus.org/maven/apidocs/org/logicblaze/lingo/jms/Requestor.html) interface to keep the JMS semantics.

More details [here](http://docs.codehaus.org/display/LINGO/Request+Response+with+JMS)

### Client side

So the client side creates a consumer on a temporary queue as follows...
```
// client side
Destination tempDest = session.createTemporaryQueue();
MessageConsumer responseConsumer = session.createConsumer(tempDest);
...

// send a request..
message.setJMSReplyTo(tempDest)
message.setJMSCorrelationID(myCorrelationID);

producer.send(message);
```

### Server side
```
public void onMessage(Message request) {

  Message response = session.createMessage();
  response.setJMSCorrelationID(request.getJMSCorrelationID())

  producer.send(request.getJMSReplyTo(), response)
}
```

Full Examples
-------------

### Server Side
```
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class Server implements MessageListener {
    private static int ackMode;
    private static String messageQueueName;
    private static String messageBrokerUrl;

    private Session session;
    private boolean transacted = false;
    private MessageProducer replyProducer;
    private MessageProtocol messageProtocol;

    static {
        messageBrokerUrl = "tcp://localhost:61616";
        messageQueueName = "client.messages";
        ackMode = Session.AUTO_ACKNOWLEDGE;
    }

    public Server() {
        try {
            //This message broker is embedded
            BrokerService broker = new BrokerService();
            broker.setPersistent(false);
            broker.setUseJmx(false);
            broker.addConnector(messageBrokerUrl);
            broker.start();
        } catch (Exception e) {
            //Handle the exception appropriately
        }

        //Delegating the handling of messages to another class, instantiate it before setting up JMS so it
        //is ready to handle messages
        this.messageProtocol = new MessageProtocol();
        this.setupMessageQueueConsumer();
    }

    private void setupMessageQueueConsumer() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(messageBrokerUrl);
        Connection connection;
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            this.session = connection.createSession(this.transacted, ackMode);
            Destination adminQueue = this.session.createQueue(messageQueueName);

            //Setup a message producer to respond to messages from clients, we will get the destination
            //to send to from the JMSReplyTo header field from a Message
            this.replyProducer = this.session.createProducer(null);
            this.replyProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            //Set up a consumer to consume messages off of the admin queue
            MessageConsumer consumer = this.session.createConsumer(adminQueue);
            consumer.setMessageListener(this);
        } catch (JMSException e) {
            //Handle the exception appropriately
        }
    }

    public void onMessage(Message message) {
        try {
            TextMessage response = this.session.createTextMessage();
            if (message instanceof TextMessage) {
                TextMessage txtMsg = (TextMessage) message;
                String messageText = txtMsg.getText();
                response.setText(this.messageProtocol.handleProtocolMessage(messageText));
            }

            //Set the correlation ID from the received message to be the correlation id of the response message
            //this lets the client identify which message this is a response to if it has more than
            //one outstanding message to the server
            response.setJMSCorrelationID(message.getJMSCorrelationID());

            //Send the response to the Destination specified by the JMSReplyTo field of the received message,
            //this is presumably a temporary queue created by the client
            this.replyProducer.send(message.getJMSReplyTo(), response);
        } catch (JMSException e) {
            //Handle the exception appropriately
        }
    }

    public static void main(String\[\] args) {
        new Server();
    }
}
```

### Client Side
```
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Random;

public class Client implements MessageListener {
    private static int ackMode;
    private static String clientQueueName;

    private boolean transacted = false;
    private MessageProducer producer;

    static {
        clientQueueName = "client.messages";
        ackMode = Session.AUTO_ACKNOWLEDGE;
    }

    public Client() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection;
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(transacted, ackMode);
            Destination adminQueue = session.createQueue(clientQueueName);

            //Setup a message producer to send message to the queue the server is consuming from
            this.producer = session.createProducer(adminQueue);
            this.producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            //Create a temporary queue that this client will listen for responses on then create a consumer
            //that consumes message from this temporary queue...for a real application a client should reuse
            //the same temp queue for each message to the server...one temp queue per client
            Destination tempDest = session.createTemporaryQueue();
            MessageConsumer responseConsumer = session.createConsumer(tempDest);

            //This class will handle the messages to the temp queue as well
            responseConsumer.setMessageListener(this);

            //Now create the actual message you want to send
            TextMessage txtMessage = session.createTextMessage();
            txtMessage.setText("MyProtocolMessage");

            //Set the reply to field to the temp queue you created above, this is the queue the server
            //will respond to
            txtMessage.setJMSReplyTo(tempDest);

            //Set a correlation ID so when you get a response you know which sent message the response is for
            //If there is never more than one outstanding message to the server then the
            //same correlation ID can be used for all the messages...if there is more than one outstanding
            //message to the server you would presumably want to associate the correlation ID with this
            //message somehow...a Map works good
            String correlationId = this.createRandomString();
            txtMessage.setJMSCorrelationID(correlationId);
            this.producer.send(txtMessage);
        } catch (JMSException e) {
            //Handle the exception appropriately
        }
    }

    private String createRandomString() {
        Random random = new Random(System.currentTimeMillis());
        long randomLong = random.nextLong();
        return Long.toHexString(randomLong);
    }

    public void onMessage(Message message) {
        String messageText = null;
        try {
            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                messageText = textMessage.getText();
                System.out.println("messageText = " + messageText);
            }
        } catch (JMSException e) {
            //Handle the exception appropriately
        }
    }

    public static void main(String\[\] args) {
        new Client();
    }
}
```

### Protocol Class

This class is needed to run the client/server example above. Delegating the handling of messages to a seperate class is solely a personal preference.
```
public class MessageProtocol {
    public String handleProtocolMessage(String messageText) {
        String responseText;
        if ("MyProtocolMessage".equalsIgnoreCase(messageText)) {
            responseText = "I recognize your protocol message";
        } else {
            responseText = "Unknown protocol message: " + messageText;
        }
        
        return responseText;
    }
}
```
