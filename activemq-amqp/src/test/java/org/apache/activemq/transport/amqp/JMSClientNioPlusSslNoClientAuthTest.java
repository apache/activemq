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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.junit.Test;

/**
 * Verifies that an amqp+nio+ssl connector with neither needClientAuth nor
 * wantClientAuth set does not request a client certificate. Pairs with
 * JMSClientNioPlusSslClientAuthTest, which checks that the bare flags turn
 * client auth on.
 */
public class JMSClientNioPlusSslNoClientAuthTest extends JMSClientTestSupport {

    private volatile boolean connectionEstablished = false;
    private volatile boolean hasCertificate = false;

    @Override
    protected boolean isUseTcpConnector() {
        return false;
    }

    @Override
    protected boolean isUseNioPlusSslConnector() {
        return true;
    }

    @Override
    protected URI getBrokerURI() {
        return amqpNioPlusSslURI;
    }

    // No needClientAuth or wantClientAuth set, so the connector requests no
    // client authentication.

    @Override
    protected void addAdditionalPlugins(List<BrokerPlugin> plugins) throws Exception {
        super.addAdditionalPlugins(plugins);
        plugins.add(new BrokerPlugin() {

            @Override
            public Broker installPlugin(Broker broker) throws Exception {
                return new BrokerFilter(broker) {

                    @Override
                    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
                        super.addConnection(context, info);
                        connectionEstablished = true;
                        hasCertificate = info.getTransportContext() instanceof X509Certificate[];
                    }
                };
            }
        });
    }

    @Test(timeout = 60000)
    public void testClientCertificateNotRequestedWithoutClientAuth() throws Exception {
        connection = createConnection();

        assertTrue("the amqp+nio+ssl connection should have been established", connectionEstablished);
        assertFalse("amqp+nio+ssl must not request a client certificate when neither "
                + "needClientAuth nor wantClientAuth is set", hasCertificate);
    }
}
