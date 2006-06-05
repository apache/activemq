/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Session;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.Properties;

public class JmsClientSupport extends JmsFactorySupport {
    private static final Log log = LogFactory.getLog(JmsClientSupport.class);

    private static final String PREFIX_CONFIG_CLIENT = "client.";
    public  static final String SESSION_AUTO_ACKNOWLEDGE    = "autoAck";
    public  static final String SESSION_CLIENT_ACKNOWLEDGE  = "clientAck";
    public  static final String SESSION_DUPS_OK_ACKNOWLEDGE = "dupsAck";
    public  static final String SESSION_TRANSACTED          = "transacted";

    protected Properties    clientSettings = new Properties();
    protected Connection    jmsConnection;
    protected Session       jmsSession;

    // Client settings
    protected String  spiClass;
    protected boolean sessTransacted = false;
    protected String  sessAckMode    = SESSION_AUTO_ACKNOWLEDGE;
    protected String  destName       = "TEST.FOO";
    protected int     destCount      = 1;
    protected boolean destComposite  = false;

    public ConnectionFactory createConnectionFactory() throws JMSException {
         return super.createConnectionFactory(getSpiClass());
    }

    public Connection getConnection() throws JMSException {
        if (jmsConnection == null) {
            jmsConnection = createConnectionFactory().createConnection();
        }
        return jmsConnection;
    }

    public Session getSession() throws JMSException {
        if (jmsSession == null) {
            int ackMode;
            if (getSessAckMode().equalsIgnoreCase(SESSION_AUTO_ACKNOWLEDGE)) {
                ackMode = Session.AUTO_ACKNOWLEDGE;
            } else if (getSessAckMode().equalsIgnoreCase(SESSION_CLIENT_ACKNOWLEDGE)) {
                ackMode = Session.CLIENT_ACKNOWLEDGE;
            } else if (getSessAckMode().equalsIgnoreCase(SESSION_DUPS_OK_ACKNOWLEDGE)) {
                ackMode = Session.DUPS_OK_ACKNOWLEDGE;
            } else if (getSessAckMode().equalsIgnoreCase(SESSION_TRANSACTED)) {
                ackMode = Session.SESSION_TRANSACTED;
            } else {
                ackMode = Session.AUTO_ACKNOWLEDGE;
            }
            jmsSession = getConnection().createSession(isSessTransacted(), ackMode);
        }
        return jmsSession;
    }

    public Destination[] createDestination() throws JMSException {
        Destination[] dest = new Destination[getDestCount()];
        for (int i=0; i<getDestCount(); i++) {
            dest[i] = createDestination(getDestName() + "." + i);
        }

        if (isDestComposite()) {
            return new Destination[] {createDestination(getDestName() + ".>")};
        } else {
            return dest;
        }
    }

    public Destination createDestination(String name) throws JMSException {
        if (name.startsWith("queue://")) {
            return getSession().createQueue(name.substring("queue://".length()));
        } else if (name.startsWith("topic://")) {
            return getSession().createTopic(name.substring("topic://".length()));
        } else {
            return getSession().createTopic(name);
        }
    }

    public String getSpiClass() {
        return spiClass;
    }

    public void setSpiClass(String spiClass) {
        this.spiClass = spiClass;
    }

    public boolean isSessTransacted() {
        return sessTransacted;
    }

    public void setSessTransacted(boolean sessTransacted) {
        this.sessTransacted = sessTransacted;
    }

    public String getSessAckMode() {
        return sessAckMode;
    }

    public void setSessAckMode(String sessAckMode) {
        this.sessAckMode = sessAckMode;
    }

    public String getDestName() {
        return destName;
    }

    public void setDestName(String destName) {
        this.destName = destName;
    }

    public int getDestCount() {
        return destCount;
    }

    public void setDestCount(int destCount) {
        this.destCount = destCount;
    }

    public boolean isDestComposite() {
        return destComposite;
    }

    public void setDestComposite(boolean destComposite) {
        this.destComposite = destComposite;
    }

    public Properties getClientSettings() {
        return clientSettings;
    }

    public void setClientSettings(Properties clientSettings) {
        this.clientSettings = clientSettings;
        ReflectionUtil.configureClass(this, clientSettings);
    }

    public Properties getSettings() {
        Properties allSettings = new Properties(clientSettings);
        allSettings.putAll(super.getSettings());
        return allSettings;
    }

    public void setSettings(Properties settings) {
        super.setSettings(settings);
        ReflectionUtil.configureClass(this, clientSettings);
    }

    public void setProperty(String key, String value) {
        if (key.startsWith(PREFIX_CONFIG_CLIENT)) {
            clientSettings.setProperty(key, value);
        } else {
            super.setProperty(key, value);
        }
    }
}
