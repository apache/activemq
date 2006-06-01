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
import java.util.Map;
import java.lang.reflect.Constructor;

public class JmsBasicClientSupport {
    private static final Log log = LogFactory.getLog(JmsBasicClientSupport.class);

    public static final String DEFAULT_CONNECTION_FACTORY_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";

    public ConnectionFactory createConnectionFactory(String url) {
        return createConnectionFactory(DEFAULT_CONNECTION_FACTORY_CLASS, url, null);
    }

    public ConnectionFactory createConnectionFactory(String clazz, String url) {
        return createConnectionFactory(clazz, url, null);
    }

    public ConnectionFactory createConnectionFactory(String clazz, String url, Map props) {
        if (clazz == null || clazz == "") {
            throw new RuntimeException("No class definition specified to create connection factory.");
        }

        ConnectionFactory f = instantiateConnectionFactory(clazz, url);
        if (props != null) {
            ReflectionUtil.configureClass(f, props);
        }

        return f;
    }

    protected ConnectionFactory instantiateConnectionFactory(String clazz, String url) {
        try {
            Class factoryClass = Class.forName(clazz);
            Constructor c = factoryClass.getConstructor(new Class[] {String.class});
            ConnectionFactory factoryObj = (ConnectionFactory)c.newInstance(new Object[] {url});
            
            return factoryObj;
        } catch (Exception e) {
            throw new RuntimeException (e);
        }
    }
}
