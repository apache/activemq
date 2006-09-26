/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.panacya.platform.service.bus.sender;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.ejb.CreateException;
import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */

public class SenderBean implements SessionBean {
    private Log _log = LogFactory.getLog(SenderBean.class);
    
    public SenderBean() {
    }

    public void ejbCreate() throws CreateException {
    }

    public void setSessionContext(SessionContext sessionContext) throws EJBException {
    }

    public void ejbRemove() throws EJBException {
    }

    public void ejbActivate() throws EJBException {
    }

    public void ejbPassivate() throws EJBException {
    }

    public void sendMessage(String message) throws SenderException {
        try {
            send(message);
        } catch (NamingException e) {
            _log.error(e.toString(), e);
            throw new SenderException(e);
        } catch (JMSException e) {
            _log.error(e.toString(), e);
            throw new SenderException(e);
        }
    }
    
    private void send(String recMessage) throws NamingException, JMSException {
        InitialContext initCtx = new InitialContext();
        QueueConnectionFactory qcf = (QueueConnectionFactory) initCtx.lookup("java:comp/env/jms/MyQueueConnectionFactory");
        QueueConnection qcon = qcf.createQueueConnection();
        QueueSession qsession = qcon.createQueueSession(true, 0);
        Queue q = (Queue) initCtx.lookup("java:comp/env/jms/LogQueue");
        QueueSender qsender = qsession.createSender(q);
        TextMessage message = qsession.createTextMessage();
        message.setText("Message Received: " + recMessage);
        qsender.send(message);
        qsender.close();
        qsession.close();
        qcon.close();
    }
    
}
