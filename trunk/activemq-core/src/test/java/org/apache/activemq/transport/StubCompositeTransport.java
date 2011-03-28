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
package org.apache.activemq.transport;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class StubCompositeTransport extends StubTransport implements CompositeTransport
{
    private List<URI> transportURIs = new ArrayList<URI>();    
    
    /**
     * @see org.apache.activemq.transport.CompositeTransport#add(java.net.URI[])
     */
    public void add(boolean rebalance, URI[] uris)
    {
        transportURIs.addAll(Arrays.asList(uris));
    }

    /**
     * @see org.apache.activemq.transport.CompositeTransport#remove(java.net.URI[])
     */
    public void remove(boolean rebalance, URI[] uris)
    {
        transportURIs.removeAll(Arrays.asList(uris));
    }

    public URI[] getTransportURIs()
    {
        return transportURIs.toArray(new URI[0]);
    }
}
