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

package org.apache.activemq.transport.http.marshallers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.activemq.wireformat.WireFormat;

public class HttpWireFormatMarshaller implements HttpTransportMarshaller
{
    private final WireFormat wireFormat;

    public HttpWireFormatMarshaller(final WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    @Override
    public void marshal(final Object command, final OutputStream outputStream) throws IOException {
        final DataOutputStream out = new DataOutputStream(outputStream);
        wireFormat.marshal(command, out);
        out.flush();
    }

    @Override
    public Object unmarshal(final InputStream stream) throws IOException {
        return wireFormat.unmarshal(new DataInputStream(stream));
    }

    @Override
    public WireFormat getWireFormat() {
        return wireFormat;
    }
}
