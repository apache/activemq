package org.apache.activemq.transport.amqp;

import org.apache.activemq.command.*;
import org.apache.qpid.proton.jms.JMSVendor;

import javax.jms.*;
import javax.jms.Message;
import java.util.Set;

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
    @SuppressWarnings("deprecation")
    public Destination createDestination(String name) {
        return super.createDestination(name, Destination.class);
    }

    public <T extends Destination> T createDestination(String name, Class<T> kind) {
        if( kind == Queue.class ) {
            return kind.cast(new ActiveMQQueue(name));
        }
        if( kind == Topic.class ) {
            return kind.cast(new ActiveMQTopic(name));
        }
        if( kind == TemporaryQueue.class ) {
            return kind.cast(new ActiveMQTempQueue(name));
        }
        if( kind == TemporaryTopic.class ) {
            return kind.cast(new ActiveMQTempTopic(name));
        }
        return kind.cast(ActiveMQDestination.createDestination(name, ActiveMQDestination.QUEUE_TYPE));
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

    @Override
    public void setJMSXDeliveryCount(Message msg, long value) {
        ((ActiveMQMessage)msg).setRedeliveryCounter((int) value);
    }

    @Override
    public String toAddress(Destination dest) {
        return ((ActiveMQDestination)dest).getQualifiedName();
    }
}
