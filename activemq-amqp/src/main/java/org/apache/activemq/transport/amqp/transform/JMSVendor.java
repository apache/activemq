package org.apache.activemq.transport.amqp.transform;

import javax.jms.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class JMSVendor {

    public abstract BytesMessage createBytesMessage();

    public abstract StreamMessage createStreamMessage();

    public abstract Message createMessage();

    public abstract TextMessage createTextMessage();

    public abstract ObjectMessage createObjectMessage();

    public abstract MapMessage createMapMessage();

    public abstract void setJMSXUserID(Message msg, String value);

    public abstract Destination createDestination(String name);

    public abstract void setJMSXGroupID(Message msg, String groupId);

    public abstract void setJMSXGroupSequence(Message msg, int i);

    public abstract void setJMSXDeliveryCount(Message rc, long l);

    public abstract String toAddress(Destination msgDestination);

}
