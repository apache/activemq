/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.LinkedList;

/**
 * A simple pool of JMS Session objects intended for use by Queue browsers.
 * 
 * @version $Revision$
 */
public class SessionPool {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private LinkedList sessions = new LinkedList();

    public Connection getConnection() throws JMSException {
        if (connection == null) {
            connection = getConnectionFactory().createConnection();
            connection.start();
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            // TODO support remote brokers too
            connectionFactory = new ActiveMQConnectionFactory("vm://localhost");
        }
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }


    public Session borrowSession() throws JMSException {
        Session answer = null;
        synchronized (sessions) {
            if (sessions.isEmpty()) {
                answer = createSession();
            }
            else {
                answer = (Session) sessions.removeLast();
            }
        }
        return answer;
    }

    protected void returnSession(Session session) {
        if (session != null) {
            synchronized (sessions) {
                sessions.add(session);
            }
        }
    }

    protected Session createSession() throws JMSException {
        return getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

}
