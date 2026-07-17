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
package org.apache.activemq;

import java.net.URI;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.annotation.Experimental;
import org.apache.activemq.management.JMSStatsImpl;
import org.apache.activemq.transport.Transport;

/**
 * Connection factory that creates {@link SharedTopicConnection} instances,
 * enabling JMS 3.1 shared topic subscription support throughout the
 * connection → session → consumer chain.
 *
 * <p>Requires a broker running {@code SharedTopicBrokerService}; shared
 * subscriptions negotiate OpenWire v13, which a default broker does not enable.
 */
@Experimental("Tech Preview for JMS 3.1 shared topic subscriptions")
public class SharedTopicConnectionFactory extends ActiveMQConnectionFactory {

    public SharedTopicConnectionFactory() {
        super();
    }

    public SharedTopicConnectionFactory(String brokerURL) {
        super(brokerURL);
    }

    public SharedTopicConnectionFactory(URI brokerURL) {
        super(brokerURL);
    }

    public SharedTopicConnectionFactory(String userName, String password, URI brokerURL) {
        super(userName, password, brokerURL);
    }

    public SharedTopicConnectionFactory(String userName, String password, String brokerURL) {
        super(userName, password, brokerURL);
    }

    @Override
    protected ActiveMQConnection createActiveMQConnection(Transport transport,
            JMSStatsImpl stats) throws Exception {
        SharedTopicConnection connection = new SharedTopicConnection(transport,
                getClientIdGenerator(), getConnectionIdGenerator(), stats);
        connection.setStrictCompliance(isStrictCompliance());
        return connection;
    }
}
