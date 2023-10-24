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

import static org.junit.Assert.assertTrue;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.util.ByteSequence;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test that Openwire marshalling will validate Throwable types during
 * unmarshalling commands that contain a Throwable
 */
@RunWith(Parameterized.class)
public class OpenWireValidationTest {

    protected final int version;

    @Parameters(name = "version={0}")
    public static Collection<Object[]> data() {
        List<Integer> versions = Arrays.asList(1, 9, 10, 11, 12);
        List<Object[]> versionObjs = new ArrayList<>();
        for (int i : versions) {
            versionObjs.add(new Object[]{i});
        }

        // Sanity check to make sure the latest generated version is contained in the list
        // This will make sure that we don't forget to update this test to include
        // any future versions that are generated
        assertTrue("List of Openwire versions does not include latest version",
            versions.contains((int)CommandTypes.PROTOCOL_VERSION));

        return versionObjs;
    }

    public OpenWireValidationTest(int version) {
        this.version = version;
    }

    @Test
    public void testOpenwireThrowableValidation() throws Exception {
        // Create a format which will use loose encoding by default
        // The code for handling exception creation is shared between both
        // tight/loose encoding so only need to test 1
        OpenWireFormat format = new OpenWireFormat();

        // Override the marshaller map with a custom impl to purposely marshal a class type that is
        // not a Throwable for testing the unmarshaller
        Class<?> marshallerFactory = getMarshallerFactory();
        Method createMarshallerMap = marshallerFactory.getMethod("createMarshallerMap", OpenWireFormat.class);
        DataStreamMarshaller[] map = (DataStreamMarshaller[]) createMarshallerMap.invoke(marshallerFactory, format);
        map[ExceptionResponse.DATA_STRUCTURE_TYPE] = getExceptionMarshaller();
        // This will trigger updating the marshaller from the marshaller map with the right version
        format.setVersion(version);

        // Build the response and try to unmarshal which should give an IllegalArgumentExeption on unmarshall
        // as the test marshaller should have encoded a class type that is not a Throwable
        ExceptionResponse r = new ExceptionResponse();
        r.setException(new Exception());
        ByteSequence bss = format.marshal(r);
        ExceptionResponse response = (ExceptionResponse) format.unmarshal(bss);

        assertTrue(response.getException() instanceof IllegalArgumentException);
        assertTrue(response.getException().getMessage().contains("is not assignable to Throwable"));
    }

    static class NotAThrowable {
        private String message;

        public NotAThrowable(String message) {
            this.message = message;
        }

        public NotAThrowable() {
        }
    }

    private Class<?> getMarshallerFactory() throws ClassNotFoundException {
        return Class.forName("org.apache.activemq.openwire.v" + version + ".MarshallerFactory");
    }

    // Create test marshallers for all non-legacy versions that will encode NotAThrowable
    // instead of the exception type for testing purposes
    protected DataStreamMarshaller getExceptionMarshaller() {
        switch (version) {
            case 12:
                return new org.apache.activemq.openwire.v12.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 11:
                return new org.apache.activemq.openwire.v11.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 10:
                return new org.apache.activemq.openwire.v10.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 9:
                return new org.apache.activemq.openwire.v9.ExceptionResponseMarshaller() {
                    @Override
                    protected void looseMarshalThrowable(OpenWireFormat wireFormat, Throwable o,
                        DataOutput dataOut) throws IOException {
                        dataOut.writeBoolean(o != null);
                        looseMarshalString(NotAThrowable.class.getName(), dataOut);
                        looseMarshalString(o.getMessage(), dataOut);
                    }
                };
            case 1:
                return new org.apache.activemq.openwire.v1.ExceptionResponseMarshaller() {
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

}
