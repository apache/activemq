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

import org.apache.activemq.transport.http.HttpTransportFactory;
import org.apache.activemq.transport.http.marshallers.HttpTransportMarshaller;
import org.apache.activemq.wireformat.WireFormat;
import org.hamcrest.CoreMatchers;

import java.util.LinkedList;

import static org.junit.Assert.assertThat;

/**
 * Ensures that all transports created by this factory are of the expected type.
 */
public class AssertingTransportFactory extends HttpTransportFactory {
    private final Class<?> expectedMarshallerType;
    private final LinkedList<SpyMarshaller> spyMarshallers = new LinkedList<>();

    public AssertingTransportFactory(final String wireFormat, final Class<?> expectedMarshallerType) {
        super(wireFormat);
        this.expectedMarshallerType = expectedMarshallerType;
    }

    @Override
    protected HttpTransportMarshaller createMarshaller(final WireFormat wireFormat)
    {
        final HttpTransportMarshaller marshaller = super.createMarshaller(wireFormat);
        assertThat("Unexpected marshaller used", marshaller, CoreMatchers.instanceOf(expectedMarshallerType));
        final SpyMarshaller spyMarshaller = new SpyMarshaller(marshaller);
        spyMarshallers.add(spyMarshaller);
        return spyMarshaller;
    }

    public LinkedList<SpyMarshaller> getSpyMarshallers() {
        return spyMarshallers;
    }
}
