package org.apache.activemq.transport.amqp;

import org.apache.activemq.command.*;
import org.apache.activemq.transport.amqp.transform.JMSVendor;

import javax.jms.*;
import javax.jms.Message;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ActiveMQJMSVendor extends JMSVendor {

    final public static ActiveMQJMSVendor INSTANCE = new ActiveMQJMSVendor();

    private ActiveMQJMSVendor() {}

    @Override
    public BytesMessage createBytesMessage() {
        return new ActiveMQBytesMessage();
    }

    @Override
    public StreamMessage createStreamMessage() {
        return new ActiveMQStreamMessage();
    }

    @Override
    public Message createMessage() {
        return new ActiveMQMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        return new ActiveMQTextMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return new ActiveMQObjectMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return new ActiveMQMapMessage();
    }

    @Override
    public Destination createDestination(String name) {
        return ActiveMQDestination.createDestination(name, ActiveMQDestination.QUEUE_TYPE);
    }

    @Override
    public void setJMSXUserID(Message msg, String value) {
        ((ActiveMQMessage)msg).setUserID(value);
    }

    @Override
    public void setJMSXGroupID(Message msg, String value) {
        ((ActiveMQMessage)msg).setGroupID(value);
    }

    @Override
    public void setJMSXGroupSequence(Message msg, int value) {
        ((ActiveMQMessage)msg).setGroupSequence(value);
    }
}
