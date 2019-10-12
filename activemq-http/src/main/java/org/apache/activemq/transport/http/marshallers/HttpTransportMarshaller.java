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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.activemq.wireformat.WireFormat;

/**
 * A generic interface for marshallers used for HTTP communication.
 */
public interface HttpTransportMarshaller
{
    /**
     * The implementations of this method should be able to marshall the supplied object into the output stream.
     *
     * @param command the object to marshall
     * @param outputStream output stream for the serialised form.
     * @throws IOException
     */
    void marshal(final Object command, final OutputStream outputStream) throws IOException;

    /**
     * The implementations of this method handle unmarshalling of objects from a wire format into Java objects.
     *
     * @param stream the stream with the serialised form of an object
     * @return the deserialised object
     * @throws IOException
     */
    Object unmarshal(final InputStream stream) throws IOException;

    /**
     *
     * @return the wire format used by this marshaller
     */
    WireFormat getWireFormat();
}
