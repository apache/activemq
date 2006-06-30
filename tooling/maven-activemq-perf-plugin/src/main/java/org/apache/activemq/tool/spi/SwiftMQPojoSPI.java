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
package org.apache.activemq.tool.spi;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.NamingException;
import java.util.Properties;


public class SwiftMQPojoSPI extends ClassLoaderSPIConnectionFactory {
    public static final String KEY_BROKER_URL = "brokerUrl";
    public static final String KEY_DEST_TYPE = "destType";
    public static final String DEFAULT_URL = "smqp://localhost:4001";
    public static final String SWIFTMQ_CONTEXT = "com.swiftmq.jndi.InitialContextFactoryImpl";
    public static final String SMQP = "com.swiftmq.jms.smqp";

    protected ConnectionFactory instantiateConnectionFactory(Properties settings) throws Exception {
        String destType = settings.getProperty(KEY_DEST_TYPE);
        ConnectionFactory factory;

        InitialContext context = getInitialContext(settings);

        if (destType != null && destType == "queue") {
            factory = (ConnectionFactory) context.lookup("QueueConnectionFactory");
        } else {
            factory = (ConnectionFactory) context.lookup("TopicConnectionFactory");
        }

        return factory;
    }

    public void configureConnectionFactory(ConnectionFactory jmsFactory, Properties settings) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public InitialContext getInitialContext(Properties settings) throws Exception {
        String url = settings.getProperty(KEY_BROKER_URL);

        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, SWIFTMQ_CONTEXT);
        properties.put(Context.URL_PKG_PREFIXES, SMQP);

        if (url != null && url.length() > 0) {
            properties.put(Context.PROVIDER_URL, url);
        } else {
            properties.put(Context.PROVIDER_URL, DEFAULT_URL);
        }

        try {
            return new InitialContext(properties);
        } catch (NamingException e) {
            throw new JMSException("Error creating InitialContext ", e.toString());
        }
    }

}
