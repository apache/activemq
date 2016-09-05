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

package org.apache.activemq.transport.http.openwire;

import org.apache.activemq.transport.http.marshallers.HttpTransportMarshaller;
import org.apache.activemq.wireformat.WireFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class SpyMarshaller implements HttpTransportMarshaller {
    private final HttpTransportMarshaller marshaller;

    private final AtomicInteger marshallCalls = new AtomicInteger();
    private final AtomicInteger unmarshallCalls = new AtomicInteger();

    public SpyMarshaller(final HttpTransportMarshaller marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public void marshal(final Object command, final OutputStream outputStream) throws IOException {
        marshallCalls.incrementAndGet();
        marshaller.marshal(command, outputStream);
    }

    @Override
    public Object unmarshal(final InputStream stream) throws IOException {
        unmarshallCalls.incrementAndGet();
        return marshaller.unmarshal(stream);
    }

    public int getMarshallCallsCnt()
    {
        return marshallCalls.get();
    }

    public int getUnmarshallCallsCnt()
    {
        return unmarshallCalls.get();
    }

    @Override
    public WireFormat getWireFormat() {
        return marshaller.getWireFormat();
    }
}
