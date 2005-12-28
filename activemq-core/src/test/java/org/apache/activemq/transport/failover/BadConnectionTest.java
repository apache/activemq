/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activemq.transport.failover;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;

import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

/**
 *
 * @version $Revision: 1.1 $
 */
public class BadConnectionTest extends TestCase {

    protected Transport transport;

    public void testConnectingToUnavailableServer() throws Exception {
        try {
            transport.asyncRequest(new ActiveMQMessage());
            fail("This should never succeed");
        }
        catch (IOException e) {
            System.out.println("Caught expected exception: " + e);
            e.printStackTrace();
        }
    }
    protected Transport createTransport() throws Exception {
        return TransportFactory.connect(new URI("failover://(tcp://doesNotExist:1234)?useExponentialBackOff=false&maxReconnectAttempts=3&initialReconnectDelay=100"));
    }

    protected void setUp() throws Exception {
        transport = createTransport();
    }

    protected void tearDown() throws Exception {
        if (transport != null) { 
            transport.stop();
        }
    }
    
    
    
}
