/**
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
package org.apache.activemq.transport.tcp;

import junit.framework.TestCase;
import org.apache.activemq.transport.*;

import java.net.Socket;
import java.net.URI;
import java.util.HashMap;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class TcpTransportServerTest extends TestCase{

    public void testDefaultPropertiesSetOnTransport() throws Exception {
        TcpTransportServer server = (TcpTransportServer) TransportFactory.bind(new URI("tcp://localhost:61616?trace=true"));
        server.setTransportOption(new HashMap<String, Object>());

        server.setAcceptListener(new TransportAcceptListener() {
            @Override
            public void onAccept(Transport transport) {
                assertTrue("This transport does not have a TransportLogger!!", hasTransportLogger(transport));
            }

            @Override
            public void onAcceptError(Exception error) {
                fail("Should not have received an error!");
            }
        });

        server.start();


        Socket socket = new Socket("localhost", 61616);
        server.handleSocket(socket);
        server.stop();


    }

    private boolean hasTransportLogger(Transport transport) {
        boolean end = false;

        Transport current = transport;
        while(!end) {

            if (current instanceof TransportFilter) {
                TransportFilter filter = (TransportFilter) current;

                if(filter instanceof TransportLogger){
                    return true;
                }

                current = filter.getNext();
            }
            else {
                end = true;
            }
        }

        return false;
    }
}
