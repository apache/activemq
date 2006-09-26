/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.transport.jxta;

import org.activemq.io.WireFormat;
import org.activemq.transport.TransportChannel;
import org.activemq.transport.TransportChannelFactorySupport;

import javax.jms.JMSException;
import java.net.URI;

/**
 * A JXTA implementation of a TransportChannelFactory
 *
 * @version $Revision: 1.1 $
 */
public class JxtaTransportChannelFactory extends TransportChannelFactorySupport {

    /**
     * Create a Channel to a remote Node - e.g. a Broker
     *
     * @param wireFormat
     * @param remoteLocation
     * @return the TransportChannel bound to the remote node
     * @throws JMSException
     */
    public TransportChannel create(WireFormat wireFormat, URI remoteLocation) throws JMSException {
        return populateProperties(new JxtaTransportChannel(wireFormat, remoteLocation), remoteLocation);
    }

    /**
     * Create a Channel to a remote Node - e.g. a Broker
     *
     * @param wireFormat
     * @param remoteLocation
     * @param localLocation  -
     *                       e.g. local InetAddress and local port
     * @return the TransportChannel bound to the remote node
     * @throws JMSException
     */
    public TransportChannel create(WireFormat wireFormat, URI remoteLocation, URI localLocation) throws JMSException {
        return populateProperties(new JxtaTransportChannel(wireFormat, remoteLocation, localLocation), remoteLocation);
    }

    public boolean requiresEmbeddedBroker() {
        return false;
    }
}