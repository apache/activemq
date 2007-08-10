package org.apache.activemq.bugs;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

public class MessageSender {
    private MessageProducer producer;
    private Session session;

    public MessageSender(String queueName,Connection connection, boolean useTransactedSession) throws Exception {
        session = useTransactedSession ? connection.createSession(true, Session.SESSION_TRANSACTED) : connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createQueue(queueName));
    }

    public void send(String payload) throws Exception {
        ObjectMessage message = session.createObjectMessage();
        message.setObject(payload);
        producer.send(message);
        if (session.getTransacted()) {
            session.commit();
        }
    }
}
