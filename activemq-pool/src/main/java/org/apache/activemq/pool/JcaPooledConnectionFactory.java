/*
 * Copyright 2006 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.pool;

import java.io.IOException;
import javax.jms.Connection;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.jms.pool.ConnectionPool;
import org.apache.activemq.jms.pool.JcaConnectionPool;
import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JcaPooledConnectionFactory extends XaPooledConnectionFactory {
    private static final transient Logger LOG = LoggerFactory.getLogger(JcaPooledConnectionFactory.class);

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    protected ConnectionPool createConnectionPool(Connection connection) {
        return new JcaConnectionPool(connection, getTransactionManager(), getName()) {

            @Override
            protected Connection wrap(final Connection connection) {
                // Add a transport Listener so that we can notice if this connection
                // should be expired due to a connection failure.
                ((ActiveMQConnection)connection).addTransportListener(new TransportListener() {
                    @Override
                    public void onCommand(Object command) {
                    }

                    @Override
                    public void onException(IOException error) {
                        synchronized (this) {
                            setHasExpired(true);
                            // only log if not stopped
                            if (!stopped.get()) {
                                LOG.info("Expiring connection " + connection + " on IOException: " + error.getMessage());
                                // log stacktrace at debug level
                                LOG.debug("Expiring connection " + connection + " on IOException: ", error);
                            }
                        }
                    }

                    @Override
                    public void transportInterupted() {
                    }

                    @Override
                    public void transportResumed() {
                    }
                });

                // make sure that we set the hasFailed flag, in case the transport already failed
                // prior to the addition of our new TransportListener
                setHasExpired(((ActiveMQConnection) connection).isTransportFailed());

                // may want to return an amq EnhancedConnection
                return connection;
            }

            @Override
            protected void unWrap(Connection connection) {
                if (connection != null) {
                    ((ActiveMQConnection)connection).cleanUpTempDestinations();
                }
            }
        };
    }
}
