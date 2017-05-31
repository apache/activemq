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
package org.apache.activemq.transport.amqp;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the JMS client when connected to the NIO+SSL transport.
 */
public class JMSClientNioPlusSslTest extends JMSClientSslTest {
    protected static final Logger LOG = LoggerFactory.getLogger(JMSClientNioPlusSslTest.class);

    @Override
    protected URI getBrokerURI() {
        return amqpNioPlusSslURI;
    }

    @Override
    protected boolean isUseTcpConnector() {
        return false;
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return true;
    }

    @Override
    protected String getTargetConnectorName() {
        return "amqp+nio+ssl";
    }
}
