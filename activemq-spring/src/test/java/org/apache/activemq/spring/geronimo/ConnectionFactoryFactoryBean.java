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
package org.apache.activemq.spring.geronimo;

import jakarta.resource.ResourceException;
import jakarta.resource.spi.ConnectionManager;
import jakarta.resource.spi.ManagedConnectionFactory;

import org.springframework.beans.factory.FactoryBean;

/**
 * @org.apache.xbean.XBean element="connectionFactory"
 */
public class ConnectionFactoryFactoryBean implements FactoryBean {
    private ManagedConnectionFactory managedConnectionFactory;
    private ConnectionManager connectionManager;
    private Object connectionFactory;

    public ManagedConnectionFactory getManagedConnectionFactory() {
        return managedConnectionFactory;
    }

    public void setManagedConnectionFactory(ManagedConnectionFactory managedConnectionFactory) {
        this.managedConnectionFactory = managedConnectionFactory;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public Object getObject() throws Exception {
        return getConnectionFactory();
    }

    public Class<?> getObjectType() {
        try {
            Object connectionFactory = getConnectionFactory();
            if (connectionFactory != null) {
                return connectionFactory.getClass();
            }
        } catch (ResourceException e) {
        }
        return null;
    }

    public boolean isSingleton() {
        return true;
    }

    public Object getConnectionFactory() throws ResourceException {
        // we must initialize the connection factory outside of the getObject method since spring needs the
        // connetion factory type for autowiring before we have created the bean
        if (connectionFactory == null) {
            // START SNIPPET: cf
            if (managedConnectionFactory == null) {
                throw new NullPointerException("managedConnectionFactory is null");
            }
            if (connectionManager != null) {
                connectionFactory = managedConnectionFactory.createConnectionFactory(connectionManager);
            } else {
                connectionFactory = managedConnectionFactory.createConnectionFactory();
            }
            // END SNIPPET: cf
        }
        return connectionFactory;
    }

}
