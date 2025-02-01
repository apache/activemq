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
package org.apache.activemq.openwire;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.activemq.util.ByteSequence;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test that Openwire marshalling for legacy versions will validate certain commands correctly
 */
@RunWith(Parameterized.class)
public class OpenWireLegacyValidationTest extends OpenWireValidationTest {


    // Run through version 2 - 8 which are legacy
    @Parameters(name = "version={0}")
    public static Collection<Object[]> data() {
        List<Object[]> versions = new ArrayList<>();
        for (int i = 2; i <= 8; i++) {
            versions.add(new Object[]{i});
        }
        return versions;
    }

    public OpenWireLegacyValidationTest(int version) {
        super(version);
    }

    // Create test marshallers for all legacy versions that will encode NotAThrowable
    // instead of the exception type for testing purposes
    protected DataStreamMarshaller getExceptionMarshaller() {
        switch (version) {
            case 2:
                return new org.apache.activemq.openwire.v2.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 3:
                return new org.apache.activemq.openwire.v3.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 4:
                return new org.apache.activemq.openwire.v4.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 5:
                return new org.apache.activemq.openwire.v5.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 6:
                return new org.apache.activemq.openwire.v6.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 7:
                return new org.apache.activemq.openwire.v7.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 8:
                return new org.apache.activemq.openwire.v8.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            default:
                throw new IllegalArgumentException("Unknown openwire version of " + version);
        }
    }

    protected DataStreamMarshaller getWireFormatInfoMarshaller() {
        switch (version) {
            case 2:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v2.WireFormatInfoMarshaller());
            case 3:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v3.WireFormatInfoMarshaller());
            case 4:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v4.WireFormatInfoMarshaller());
            case 5:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v5.WireFormatInfoMarshaller());
            case 6:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v6.WireFormatInfoMarshaller());
            case 7:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v7.WireFormatInfoMarshaller());
            case 8:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v8.WireFormatInfoMarshaller());
            default:
                throw new IllegalArgumentException("Unknown OpenWire version of " + version);
        }
    }

    protected DataStreamMarshaller getPartialCommandMarshaller() {
        switch (version) {
            case 2:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v2.PartialCommandMarshaller());
            case 3:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v3.PartialCommandMarshaller());
            case 4:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v4.PartialCommandMarshaller());
            case 5:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v5.PartialCommandMarshaller());
            case 6:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v6.PartialCommandMarshaller());
            case 7:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v7.PartialCommandMarshaller());
            case 8:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v8.PartialCommandMarshaller());
            default:
                throw new IllegalArgumentException("Unknown OpenWire version of " + version);
        }
    }

}
