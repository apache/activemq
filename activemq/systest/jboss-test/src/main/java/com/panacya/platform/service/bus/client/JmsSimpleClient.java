package com.panacya.platform.service.bus.client;

import org.activemq.ActiveMQConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import javax.jms.Message;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.jms.ConnectionFactory;

import org.activemq.util.IdGenerator;

import java.util.Properties;
import java.lang.reflect.InvocationTargetException;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */
public class JmsSimpleClient {

    private static final String BROKER_URL = "tcp://localhost:61616";
    //private static final String BROKER_URL = "jnp://localhost:1099";
    private static final String SEND_CMD = "send";
    private static final String RECEIVE_CMD = "receive";
    private static final String ENDLESS_RECEIVE_CMD = "receive-non-stop";
    private static final String SEND_RECEIVE_CMD = "send-receive";

    public static final String NAMING_CONTEXT = "org.jnp.interfaces.NamingContextFactory";
    public static final String JNP_INTERFACES = "org.jnp.interfaces";


    public static void main(String[] args) {
        execute(new ClientArgs(args));
    }

    private static void execute(ClientArgs args) {
        try {
            if (SEND_CMD.equals(args.getCommand())) {
                JmsSimpleClient.sendToActiveMQ(args.getDestination(), args.getNoOfMessages());
            } else if (RECEIVE_CMD.equals(args.getCommand())) {
                JmsSimpleClient.receiveFromActiveMQ(args.getDestination(), args.getTimeout());
            } else if (ENDLESS_RECEIVE_CMD.equals(args.getCommand())) {
                JmsSimpleClient.receiveFromActiveMQ(args.getDestination());
            } else if (SEND_RECEIVE_CMD.equals(args.getCommand())) {
                JmsSimpleClient.sendToActiveMQ(args.getDestination(), args.getNoOfMessages());
                JmsSimpleClient.receiveFromActiveMQ(args.getDestination(), args.getTimeout());
            } else {
                System.err.println("Unknown command: " + args.getCommand());
                System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void sendToActiveMQ(String destinationName, int count) throws Exception {
        System.out.println("Sending to '" + destinationName + "' ...");
        JmsTemplate jt = createTemplate(destinationName);

        int i = 0;
        for (; i < count; i++) {
            jt.send(destinationName, new MessageCreator() {
                public Message createMessage(Session session) throws JMSException {
                    return session.createTextMessage("hello ActiveMQ world ");
                }
            });
        }

        System.out.println("Done sending " + count + " message/s ........");
    }

    public static void receiveFromActiveMQ(String destinationName, long timeout) throws Exception {
        System.out.println("Listening to '" + destinationName + "' ...");
        JmsTemplate jt = createTemplate(destinationName);
        jt.setReceiveTimeout(timeout);
        while (true) {
            Message aMessage = jt.receive(destinationName);
            System.out.println("...done");
            if (aMessage == null) {
                System.out.println("No message received");
            } else {
                System.out.println("Message Received: " + aMessage.toString());
            }
        }
    }

    public static void receiveFromActiveMQ(String destinationName) throws Exception {
        System.out.println("Listening to '" + destinationName + "' ...");
        JmsTemplate jt = createTemplate(destinationName);

        while (true) {
            Message aMessage = jt.receive(destinationName);
            if (aMessage == null) {
                System.out.println("No message received");
            } else {
                int messageNumber = aMessage.getIntProperty("MessageNumber");
                System.out.println("Received MessageNumber: " + messageNumber);
            }
        }
    }

    private static JmsTemplate createTemplate(String destinationName) {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(BROKER_URL);

        IdGenerator idGenerator = new IdGenerator();
        connectionFactory.setClientID(idGenerator.generateId());

        JmsTemplate jt = new JmsTemplate(connectionFactory);
        if (destinationName.startsWith("topic")) {
            jt.setPubSubDomain(true);
        }

        return jt;

    }


}
