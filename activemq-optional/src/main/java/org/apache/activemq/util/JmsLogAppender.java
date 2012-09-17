/**
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
package org.apache.activemq.util;

import java.net.URISyntaxException;
import javax.jms.Connection;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;

/**
 * A JMS 1.1 log4j appender which uses ActiveMQ by default and does not require
 * any JNDI configurations
 * 
 * 
 */
public class JmsLogAppender extends JmsLogAppenderSupport {
    private String uri = "tcp://localhost:61616";
    private String userName;
    private String password;

    public JmsLogAppender() {
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    protected Connection createConnection() throws JMSException {
        if (userName != null) {
            try {
                return ActiveMQConnection.makeConnection(userName, password, uri);
            } catch (URISyntaxException e) {
                throw new JMSException("Unable to connect to a broker using " + "userName: \'" + userName + "\' password \'" + password + "\' uri \'" + uri + "\' :: error - " + e.getMessage());
            }
        } else {
            try {
                return ActiveMQConnection.makeConnection(uri);
            } catch (URISyntaxException e) {
                throw new JMSException("Unable to connect to a broker using " + "uri \'" + uri + "\' :: error - " + e.getMessage());
            }
        }
    }
}
