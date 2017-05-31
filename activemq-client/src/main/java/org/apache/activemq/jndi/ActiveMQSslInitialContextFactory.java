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
import java.util.Hashtable;
import java.util.Properties;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.ActiveMQXASslConnectionFactory;

public class ActiveMQSslInitialContextFactory extends ActiveMQInitialContextFactory {

    /**
     * Factory method to create a new connection factory from the given
     * environment
     */
    @Override
    protected ActiveMQConnectionFactory createConnectionFactory(Hashtable environment) throws URISyntaxException {
        ActiveMQConnectionFactory answer = needsXA(environment) ? new ActiveMQXASslConnectionFactory() : new ActiveMQSslConnectionFactory();
        Properties properties = new Properties();
        properties.putAll(environment);
        answer.setProperties(properties);
        return answer;
    }
}
