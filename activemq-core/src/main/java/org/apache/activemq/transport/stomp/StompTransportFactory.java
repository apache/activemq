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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.util.IOExceptionSupport;

/**
 * A <a href="http://stomp.codehaus.org/">Stomp</a> transport factory
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class StompTransportFactory extends TransportFactory {

    public TransportServer doBind(String brokerId, URI location) throws IOException {
        try {
            URI tcpURI = new URI(
                    "tcp://"+location.getHost()+
                    (location.getPort()>=0 ? ":"+location.getPort() : "")+
                    "?wireFormat=stomp"
                    );
            return TransportFactory.bind(brokerId, tcpURI);
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

}