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
package org.apache.activemq.jndi;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

/**
 * A factory of the ActiveMQ InitialContext which contains
 * {@link ConnectionFactory} instances as well as a child context called
 * <i>destinations</i> which contain all of the current active destinations, in
 * child context depending on the QoS such as transient or durable and queue or
 * topic.
 * 
 * @version $Revision: 1.2 $
 */
public class ActiveMQInitialContextFactory implements InitialContextFactory {

    private static final String[] DEFAULT_CONNECTION_FACTORY_NAMES = {"ConnectionFactory", "QueueConnectionFactory", "TopicConnectionFactory"};

    private String connectionPrefix = "connection.";
    private String queuePrefix = "queue.";
    private String topicPrefix = "topic.";

    public Context getInitialContext(Hashtable environment) throws NamingException {
        // lets create a factory
        Map data = new ConcurrentHashMap();
        String[] names = getConnectionFactoryNames(environment);
        for (int i = 0; i < names.length; i++) {
            ActiveMQConnectionFactory factory = null;
            String name = names[i];

            try {
                factory = createConnectionFactory(name, environment);
            } catch (Exception e) {
                throw new NamingException("Invalid broker URL");

            }
            /*
             * if( broker==null ) { try { broker = factory.getEmbeddedBroker(); }
             * catch (JMSException e) { log.warn("Failed to get embedded
             * broker", e); } }
             */
            data.put(name, factory);
        }

        createQueues(data, environment);
        createTopics(data, environment);
        /*
         * if (broker != null) { data.put("destinations",
         * broker.getDestinationContext(environment)); }
         */
        data.put("dynamicQueues", new LazyCreateContext() {
            private static final long serialVersionUID = 6503881346214855588L;

            protected Object createEntry(String name) {
                return new ActiveMQQueue(name);
            }
        });
        data.put("dynamicTopics", new LazyCreateContext() {
            private static final long serialVersionUID = 2019166796234979615L;

            protected Object createEntry(String name) {
                return new ActiveMQTopic(name);
            }
        });

        return createContext(environment, data);
    }

    // Properties
    // -------------------------------------------------------------------------
    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    protected ReadOnlyContext createContext(Hashtable environment, Map data) {
        return new ReadOnlyContext(environment, data);
    }

    protected ActiveMQConnectionFactory createConnectionFactory(String name, Hashtable environment) throws URISyntaxException {
        Hashtable temp = new Hashtable(environment);
        String prefix = connectionPrefix + name + ".";
        for (Iterator iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            String key = (String)entry.getKey();
            if (key.startsWith(prefix)) {
                // Rename the key...
                temp.remove(key);
                key = key.substring(prefix.length());
                temp.put(key, entry.getValue());
            }
        }
        return createConnectionFactory(temp);
    }

    protected String[] getConnectionFactoryNames(Map environment) {
        String factoryNames = (String)environment.get("connectionFactoryNames");
        if (factoryNames != null) {
            List list = new ArrayList();
            for (StringTokenizer enumeration = new StringTokenizer(factoryNames, ","); enumeration.hasMoreTokens();) {
                list.add(enumeration.nextToken().trim());
            }
            int size = list.size();
            if (size > 0) {
                String[] answer = new String[size];
                list.toArray(answer);
                return answer;
            }
        }
        return DEFAULT_CONNECTION_FACTORY_NAMES;
    }

    protected void createQueues(Map data, Hashtable environment) {
        for (Iterator iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(queuePrefix)) {
                String jndiName = key.substring(queuePrefix.length());
                data.put(jndiName, createQueue(entry.getValue().toString()));
            }
        }
    }

    protected void createTopics(Map data, Hashtable environment) {
        for (Iterator iter = environment.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            String key = entry.getKey().toString();
            if (key.startsWith(topicPrefix)) {
                String jndiName = key.substring(topicPrefix.length());
                data.put(jndiName, createTopic(entry.getValue().toString()));
            }
        }
    }

    /**
     * Factory method to create new Queue instances
     */
    protected Queue createQueue(String name) {
        return new ActiveMQQueue(name);
    }

    /**
     * Factory method to create new Topic instances
     */
    protected Topic createTopic(String name) {
        return new ActiveMQTopic(name);
    }

    /**
     * Factory method to create a new connection factory from the given
     * environment
     */
    protected ActiveMQConnectionFactory createConnectionFactory(Hashtable environment) throws URISyntaxException {
        ActiveMQConnectionFactory answer = new ActiveMQConnectionFactory();
        Properties properties = new Properties();
        properties.putAll(environment);
        answer.setProperties(properties);
        return answer;
    }

    public String getConnectionPrefix() {
        return connectionPrefix;
    }

    public void setConnectionPrefix(String connectionPrefix) {
        this.connectionPrefix = connectionPrefix;
    }

}
