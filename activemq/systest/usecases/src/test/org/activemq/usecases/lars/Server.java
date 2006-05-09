/** 
 * 
 * Copyright 2004 Protique Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package org.activemq.usecases.lars;

import org.activemq.ActiveMQConnectionFactory;

import javax.jms.*;


/**
 * @version $Revision: 1.1 $
 */
public class Server implements MessageListener {
    private String configFile = "src/conf/activemq.xml";
    private QueueConnectionFactory cf;

    private QueueConnection queueConnection;
    private QueueSession queueSession;
    private Queue queue;
    private MessageConsumer messageConsumer;
    private QueueSender queueSender = null;

    private String messagePrefix = "X";
    private int messageMultiplier = 1;

    public Server() throws JMSException {
        super();

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setUseEmbeddedBroker(true);
        connectionFactory.setBrokerURL("vm://localhost");
        connectionFactory.setBrokerXmlConfig("file:" + configFile);

        cf = connectionFactory;

        initQueue();
        initListener();
        initSender();

        queueConnection.start();
    }

    /**
     * @param msgPfx May only contain one capital letter.
     * @throws JMSException
     */
    public Server(String msgPfx) throws JMSException {
        this();
        messagePrefix = msgPfx;

        char letter = messagePrefix.charAt(0);
        int pow = letter - 'A';

        if (pow > 0 && pow <= 'Z' - 'A') {
            messageMultiplier = (int) Math.pow(10, pow);
        }
    }

    private void initQueue() throws JMSException {
        queueConnection = cf.createQueueConnection();
        queueSession = queueConnection.createQueueSession(false, javax.jms.Session.CLIENT_ACKNOWLEDGE);
        queue = queueSession.createQueue("test_queue");
    }

    private void initSender() throws JMSException {
        queueSender = queueSession.createSender(queue);
        queueSender.setDeliveryMode(DeliveryMode.PERSISTENT);
        queueSender.setTimeToLive(5000);
    }

    private void initListener() throws JMSException {
        messageConsumer = queueSession.createReceiver(queue);
        messageConsumer.setMessageListener(this);
    }

    public void onMessage(Message message) {
        if (message instanceof MapMessage) {
            try {
                MapMessage msg = (MapMessage) message;
                String command = msg.getStringProperty("cmd");

                System.out.println(messagePrefix + command);

                msg.acknowledge();
            }
            catch (JMSException e) {
                e.printStackTrace();
            }
        }
        else {
            System.out.println("Unknown message type");
        }
    }

    public final void sendMessage(int i) throws JMSException {
        MapMessage message = getMapMessage();

        message.setStringProperty("cmd", Integer.toString(i * messageMultiplier));
        queueSender.send(message);
    }

    public MapMessage getMapMessage() throws JMSException {
        return queueSession.createMapMessage();
    }

    public void go() throws JMSException {
        for (int i = 0; i < 20; i++) {
            sendMessage(i);

            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
            }
        }
    }

    public static void main(String[] args) {
        try {
            Server s = new Server();

            s.go();

            //System.exit(0);
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
    }
}