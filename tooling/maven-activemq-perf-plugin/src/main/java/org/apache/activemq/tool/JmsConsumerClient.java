/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */

package org.apache.activemq.tool;

import javax.jms.*;

public class JmsConsumerClient extends JmsPerfClientSupport implements MessageListener {

    private ConnectionFactory factory = null;

    private String factoryClass = null;
    private String brokerUrl = null;
    private String destinationName = null;
    private Destination destination = null;

    private boolean isDurable = false;
    private boolean isAsync = true;

    public JmsConsumerClient(ConnectionFactory factory) {
        this.factory = factory;
    }

    public JmsConsumerClient(ConnectionFactory factory, String destinationName) {
        this.factory = factory;
        this.setDestinationName(destinationName);
    }

    public JmsConsumerClient(String factoryClass, String brokerUrl, String destinationName) {
        this.factoryClass = factoryClass;
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
    }

    public JmsConsumerClient(String brokerUrl, String destinationName) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
    }

    public void createConsumer(long duration) throws JMSException {
        listener.onConfigEnd(this);

        // Create connection factory
        if (factory != null) {
            createConnectionFactory(factory);
        } else if (factoryClass != null) {
            createConnectionFactory(factoryClass, brokerUrl);
        } else {
            createConnectionFactory(brokerUrl);
        }

        if (getDestination() == null) {
            setDestination(getDestinationName());
        }

        if (isDurable) {
            createDurableSubscriber((Topic) getDestination(), getClass().getName());
        } else {
            createMessageConsumer(getDestination());
        }

        if (isAsync) {
            getMessageConsumer().setMessageListener(this);
            getConnection().start();

            try {
                Thread.sleep(duration);
            } catch (InterruptedException e) {
                throw new JMSException("Error while consumer is sleeping " + e.getMessage());
            }
        } else {
            getConnection().start();
            consumeMessages(getMessageConsumer(), duration);
        }

        close(); //close consumer, session, and connection.
        listener.onConfigEnd(this);
    }

    //Increments throughput
    public void onMessage(Message message) {
        System.out.println(message.toString());
        this.incThroughput();
    }

    protected void consumeMessages(MessageConsumer consumer, long duration) throws JMSException {

        long currentTime = System.currentTimeMillis();
        long endTime = currentTime + duration;

        while (System.currentTimeMillis() <= endTime) {
            Message message = consumer.receive();
            onMessage(message);
        }
    }

    protected void close() throws JMSException {
        getMessageConsumer().close();
        getSession().close();
        getConnection().close();
    }

    public static void main(String[] args) throws Exception {
        JmsConsumerClient cons = new JmsConsumerClient("org.apache.activemq.ActiveMQConnectionFactory", "tcp://localhost:61616", "topic://TEST.FOO");
        cons.setPerfEventListener(new PerfEventAdapter());
        cons.createConsumer(20000);
    }

    // Helper Methods

    public boolean isDurable() {
        return isDurable;
    }

    public void setDurable(boolean durable) {
        isDurable = durable;
    }

    public boolean isAsync() {
        return isAsync;
    }

    public void setAsync(boolean async) {
        isAsync = async;
    }

    public String getDestinationName() {
        return this.destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public Destination getDestination() {
        return this.destination;
    }

    public void setDestination(Destination dest) {
        this.destination = dest;
    }

    public void setDestination(String destinationName) throws JMSException {

        this.setDestinationName(destinationName);
        // Create destinations
        if (this.getDestinationName().startsWith("topic://")) {
            setDestination(createTopic(getDestinationName().substring("topic://".length())));
        } else if (getDestinationName().startsWith("queue://")) {
            setDestination(createQueue(getDestinationName().substring("queue://".length())));
        } else {
            setDestination(createQueue(getDestinationName()));
        }
    }
}
