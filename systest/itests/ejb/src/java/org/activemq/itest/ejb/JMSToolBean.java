/**
 *
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
package org.activemq.itest.ejb;

import java.rmi.RemoteException;

import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is a SSB that uses an outbound JMS Resource Adapter.
 *
 * @version $Revision: 1.1 $
 */
public class JMSToolBean implements SessionBean {
    
    private static final long serialVersionUID = 3834596495499474741L;
    private static final Log log = LogFactory.getLog(JMSToolBean.class);

    private SessionContext sessionContext;
    private Context envContext;

    public void ejbCreate() {
    }
    public void ejbRemove() {
    }
    public void ejbActivate() {
    }
    public void ejbPassivate() {
    }

    public void setSessionContext(SessionContext sessionContext) {
        try {
            this.sessionContext = sessionContext;
            envContext = (Context) new InitialContext().lookup("java:comp/env");
        }
        catch (NamingException e) {
            throw new EJBException(e);
        }
    }

    public void sendTextMessage(String dest, String text) throws RemoteException, JMSException, NamingException {
        sendTextMessage(createDestination(dest), text);
    }
    
    public void sendTextMessage(Destination dest, String text) throws RemoteException, JMSException, NamingException {
        log.info("sendTextMessage start");
        Connection connection = createConnection();
        try {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(dest);
            producer.send(session.createTextMessage(text));
        } finally {
            log.info("sendTextMessage end");
            connection.close();
        }
    }

    public String receiveTextMessage(String dest, long timeout) throws RemoteException, JMSException, NamingException {
        return receiveTextMessage(createDestination(dest), timeout);        
    }
    
    public String receiveTextMessage(Destination dest, long timeout) throws RemoteException, JMSException, NamingException {
        log.info("receiveTextMessage start");
        Connection connection = createConnection();
        try {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(dest);
            TextMessage message = null;
            message = (TextMessage) consumer.receive(timeout);
            return message==null ? null : message.getText();
        } finally {
            log.info("receiveTextMessage end");
            connection.close();
        }
    }
    
    public int drain(String dest) throws RemoteException, JMSException, NamingException {
        return drain(createDestination(dest));        
    }
    
    public int drain(Destination dest) throws RemoteException, JMSException, NamingException {
        log.info("drain start");
        Connection connection = createConnection();
        try {
            
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(dest);
            int counter=0;
            while( consumer.receive(1000) != null) {
                counter++;
            }
            return counter;
            
        } finally {
            log.info("drain end");
            connection.close();
        }
    }

    private Destination createDestination(String dest) throws NamingException {
        return (Destination) envContext.lookup(dest);
    }
    private Connection createConnection() throws NamingException, JMSException {
        ConnectionFactory cf = (ConnectionFactory) envContext.lookup("jms/Default");
        Connection con = cf.createConnection();
        return con;
    }
    
}
