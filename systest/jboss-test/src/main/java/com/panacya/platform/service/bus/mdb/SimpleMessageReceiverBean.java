package com.panacya.platform.service.bus.mdb;

import com.panacya.platform.service.bus.sender.SenderClient;
import com.panacya.platform.service.bus.sender.SenderException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ejb.CreateException;
import javax.ejb.MessageDrivenBean;
import javax.ejb.MessageDrivenContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import java.rmi.RemoteException;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */

public class SimpleMessageReceiverBean implements MessageDrivenBean, MessageListener {

    private static final String SENDER_NAME = "java:comp/env/ejb/Sender";
    private Log _log = LogFactory.getLog(SimpleMessageReceiverBean.class);
    private MessageDrivenContext context;

    public SimpleMessageReceiverBean() {
        if (_log.isInfoEnabled()) {
            _log.info("SimpleMessageReceiverBean.SimpleMessageReceiverBean");
        }
    }

    public void onMessage(Message message)  {
        if (_log.isInfoEnabled()) {
            _log.info("SimpleMessageReceiverBean.onMessage");
        }
        try {
            handleMessage(message);
        } catch (JMSException e) {
            _log.error(e.toString(), e);
        } catch (NamingException e) {
            _log.error(e.toString(), e);
        } catch (RemoteException e) {
            _log.error(e.toString(), e);
        } catch (CreateException e) {
            _log.error(e.toString(), e);
        } catch (SenderException e) {
            _log.error(e.toString(), e);
        }
    }

    private void handleMessage(Message message) throws JMSException, NamingException, RemoteException, SenderException, CreateException {
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            if (_log.isInfoEnabled()) {
                _log.info("Message received: " + textMessage.getText());
            }
            send(textMessage.getText());
        } else {
            if (_log.isInfoEnabled()) {
                _log.info("Unknown message type received: " + message.toString());
            }
            send("Unknown message type: " + message.toString());
        }
    }

    public void ejbRemove() {
        if (_log.isInfoEnabled()) {
            _log.info("SimpleMessageReceiverBean.ejbRemove");
        }
    }

    public void setMessageDrivenContext(MessageDrivenContext messageDrivenContext) {
        if (_log.isInfoEnabled()) {
            _log.info("SimpleMessageReceiverBean.setMessageDrivenContext");
        }
        context = messageDrivenContext;
    }

    public void ejbCreate() {
        if (_log.isInfoEnabled()) {
            _log.info("SimpleMessageReceiverBean.ejbCreate");
        }
    }

    private void send(String recMessage) throws NamingException, RemoteException, CreateException, SenderException {
        sendToEJB(recMessage);
    }

    private void sendToEJB(String recMessage) throws NamingException, RemoteException, CreateException, SenderException {
        SenderClient senderClient = new SenderClient(SENDER_NAME);
        senderClient.sendMessage(recMessage);
    }   
}
