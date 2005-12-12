/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.pool;

import org.activemq.ActiveMQConnection;
import org.activemq.ActiveMQSession;
import org.activemq.AlreadyClosedException;
import org.activemq.util.JMSExceptionSupport;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import javax.jms.JMSException;

/**
 * Represents the session pool for a given JMS connection.
 *
 * @version $Revision: 1.1 $
 */
public class SessionPool implements PoolableObjectFactory {
    private ActiveMQConnection connection;
    private SessionKey key;
    private ObjectPool sessionPool;

    public SessionPool(ActiveMQConnection connection, SessionKey key) {
        this(connection, key, new GenericObjectPool());
    }

    public SessionPool(ActiveMQConnection connection, SessionKey key, ObjectPool sessionPool) {
        this.connection = connection;
        this.key = key;
        this.sessionPool = sessionPool;
        sessionPool.setFactory(this);
    }

    public void close() throws Exception {
        sessionPool.close();
    }
    
    public PooledSession borrowSession() throws JMSException {
        try {
            Object object = sessionPool.borrowObject();
            return (PooledSession) object;
        }
        catch (JMSException e) {
            throw e;
        }
        catch (Exception e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    // PoolableObjectFactory methods
    //-------------------------------------------------------------------------
    public Object makeObject() throws Exception {
        return new PooledSession(createSession(), sessionPool);
    }

    public void destroyObject(Object o) throws Exception {
        PooledSession session = (PooledSession) o;
        session.getSession().close();
    }

    public boolean validateObject(Object o) {
        return true;
    }

    public void activateObject(Object o) throws Exception {
    }

    public void passivateObject(Object o) throws Exception {
    }

    // Implemention methods
    //-------------------------------------------------------------------------
    protected ActiveMQConnection getConnection() throws JMSException {
        if (connection == null) {
            throw new AlreadyClosedException();
        }
        return connection;
    }

    protected ActiveMQSession createSession() throws JMSException {
        return (ActiveMQSession) getConnection().createSession(key.isTransacted(), key.getAckMode());
    }


}
