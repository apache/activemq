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
package org.apache.activemq.pool;

import java.util.Properties;
import javax.naming.NamingException;
import javax.naming.Reference;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.jndi.JNDIReferenceFactory;
import org.apache.activemq.jndi.JNDIStorableInterface;
import org.apache.activemq.pool.PooledConnectionFactory;

/**
* AmqJNDIPooledConnectionFactory.java
* Created by linus on 2008-03-07.
*/
public class AmqJNDIPooledConnectionFactory extends PooledConnectionFactory
        implements JNDIStorableInterface {
    private Properties properties;

    public AmqJNDIPooledConnectionFactory() {
        super();
    }
    
    public AmqJNDIPooledConnectionFactory(String brokerURL) {
        super(brokerURL);
    }

    public AmqJNDIPooledConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    /**
     * set the properties for this instance as retrieved from JNDI
     * 
     * @param props
     */
    public synchronized void setProperties(Properties props) {
        this.properties = props;
        buildFromProperties(props);
    }

    /**
     * Get the properties from this instance for storing in JNDI
     * 
     * @return the properties
     */
    public synchronized Properties getProperties() {
        if (this.properties == null) {
            this.properties = new Properties();
        }
        populateProperties(this.properties);
        return this.properties;
    }

    /**
     * Retrive a Reference for this instance to store in JNDI
     * 
     * @return the built Reference
     * @throws NamingException
     *             if error on building Reference
     */
    public Reference getReference() throws NamingException {
        return JNDIReferenceFactory.createReference(this.getClass().getName(),
                this);
    }

    public void buildFromProperties(Properties properties) {
        if (properties == null) {
            properties = new Properties();
        }
        ((ActiveMQConnectionFactory) getConnectionFactory())
                .buildFromProperties(properties);
        String temp = properties.getProperty("maximumActive");
        if (temp != null && temp.length() > 0) {
            setMaximumActive(Integer.parseInt(temp));
        }
        temp = properties.getProperty("maxConnections");
        if (temp != null && temp.length() > 0) {
            setMaxConnections(Integer.parseInt(temp));
        }
    }

    public void populateProperties(Properties props) {
        ((ActiveMQConnectionFactory) getConnectionFactory())
                .populateProperties(props);
        props
                .setProperty("maximumActive", Integer
                        .toString(getMaximumActive()));
        props.setProperty("maxConnections", Integer
                .toString(getMaxConnections()));
    }
}
