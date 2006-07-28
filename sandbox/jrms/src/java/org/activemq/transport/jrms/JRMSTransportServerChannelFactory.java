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
package org.activemq.transport.jrms;

import org.activemq.io.WireFormat;
import org.activemq.transport.TransportServerChannel;
import org.activemq.transport.TransportServerChannelFactory;

import javax.jms.JMSException;
import java.net.URI;

/**
 * A multicast implementation of a TransportServerChannelFactory
 * 
 * @version $Revision$
 */
public class JRMSTransportServerChannelFactory implements TransportServerChannelFactory {

    /**
     * Bind a ServerChannel to an address
     *
     * @param wireFormat
     * @param bindAddress
     * @return the TransportChannel bound to the remote node
     * @throws JMSException
     */
    public TransportServerChannel create(WireFormat wireFormat, URI bindAddress) throws JMSException {
        return new JRMSTransportServerChannel(wireFormat, bindAddress);
    }

}