/** 
 * 
 * Copyright 2004 Protique Ltd
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
 * 
 **/

import org.activemq.ActiveMQConnection;
import org.activemq.ActiveMQConnectionFactory;
import org.activemq.util.IndentPrinter;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * Abstract base class useful for implementation inheritence
 *
 * @version $Revision: 1.2 $
 */
public class ToolSupport {


    protected Destination destination;
    protected String subject = "TOOL.DEFAULT";
    protected boolean topic = false;
    protected String user = ActiveMQConnection.DEFAULT_USER;
    protected String pwd = ActiveMQConnection.DEFAULT_PASSWORD;
    protected String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    protected boolean transacted = false;
    protected boolean durable = false;
    protected String clientID;
    protected int ackMode = Session.AUTO_ACKNOWLEDGE;
    protected String consumerName = "James";


    protected Session createSession(Connection connection) throws Exception {
        Session session = connection.createSession(transacted, ackMode);
        if (topic) {
            destination = session.createTopic(subject);
        }
        else {
            destination = session.createQueue(subject);
        }
        return session;
    }

    protected Connection createConnection() throws JMSException, Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, pwd, url);
        Connection connection = connectionFactory.createConnection();
        if (durable && clientID!=null) {
            connection.setClientID(clientID);
        }
        connection.start();
        return connection;
    }

    protected void close(Connection connection, Session session) throws JMSException {
        // lets dump the stats
        dumpStats(connection);

        if (session != null) {
            session.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    protected void dumpStats(Connection connection) {
        ActiveMQConnection c = (ActiveMQConnection) connection;
        c.getConnectionStats().dump(new IndentPrinter());
    }
}
