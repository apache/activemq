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

import java.io.IOException;

import org.apache.activemq.command.Command;
import org.apache.activemq.wireformat.WireFormat;

public class MarshallingTransportFilter extends TransportFilter {

    private final WireFormat localWireFormat;
    private final WireFormat remoteWireFormat;

    public MarshallingTransportFilter(Transport next, WireFormat localWireFormat, WireFormat remoteWireFormat) {
        super(next);
        this.localWireFormat = localWireFormat;
        this.remoteWireFormat = remoteWireFormat;
    }
    
    public void oneway(Object command) throws IOException {
        next.oneway((Command) remoteWireFormat.unmarshal(localWireFormat.marshal(command)));
    }
    
    public void onCommand(Object command) {
        try {
            getTransportListener().onCommand((Command)localWireFormat.unmarshal(remoteWireFormat.marshal(command)));
        } catch (IOException e) {
            getTransportListener().onException(e);
        }
    }
    
}
