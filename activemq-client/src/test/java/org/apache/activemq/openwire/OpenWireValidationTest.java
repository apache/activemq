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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;
import javassist.util.proxy.ProxyObject;
import org.apache.activemq.command.*;
import org.apache.activemq.openwire.v1.BaseDataStreamMarshaller;
import org.apache.activemq.transport.nio.NIOInputStream;
import org.apache.activemq.util.ByteSequence;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

/**
 * Test that Openwire marshalling will validate commands correctly
 */
@RunWith(Parameterized.class)
public class OpenWireValidationTest {

    protected final int version;
    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    @Before
    public void before() {
        initialized.set(false);
    }

    @Parameters(name = "version={0}")
    public static Collection<Object[]> data() {
        List<Integer> versions = List.of(1, 9, 10, 11, 12);
        List<Object[]> versionObjs = new ArrayList<>();
        for (int i : versions) {
            versionObjs.add(new Object[]{i});
        }

        // Sanity check to make sure the latest generated version is contained in the list
        // This will make sure that we don't forget to update this test to include
        // any future versions that are generated
        assertTrue("List of Openwire versions does not include latest version",
            versions.contains((int) CommandTypes.PROTOCOL_VERSION));

        return versionObjs;
    }

    public OpenWireValidationTest(int version) {
        this.version = version;
    }

    @Test
    public void testLooseUnmarshalByteSequenceValidation() throws Exception {
        testUnmarshalByteSequenceValidation(false);
    }

    @Test
    public void testTightUnmarshalByteSequenceValidation() throws Exception {
        testUnmarshalByteSequenceValidation(true);
    }

    @Test
    public void testLooseUnmarshalByteArray() throws Exception {
        testUnmarshalByteArray(false);
    }

    @Test
    public void testTightUnmarshalByteArray() throws Exception {
        testUnmarshalByteArray(true);
    }

    // WireFormatInfo eventually delegates to BaseDataStreamMarshaller#tightUnmarshalByteSequence() and
    // BaseDataStreamMarshaller#looseUnmarshalByteSequence()
    private void testUnmarshalByteSequenceValidation(boolean tightEncoding) throws Exception {
        WireFormatInfo wfi = new WireFormatInfo();
        wfi.setProperty("prop1", "val1");
        testUnmarshal(wfi, tightEncoding);
    }

    // PartialCommand eventually delegates to BaseDataStreamMarshaller#tightUnmarshalByteArray()
    // and BaseDataStreamMarshaller#looseUnmarshalByteArray()
    private void testUnmarshalByteArray(boolean tightEncoding) throws Exception {
        PartialCommand pc = new PartialCommand();
        pc.setData("bytes".getBytes(StandardCharsets.UTF_8));
        testUnmarshal(pc, tightEncoding);
    }

    private void testUnmarshal(Command command, boolean tightEncoding) throws Exception {
        var format = setupWireFormat(tightEncoding);
        ByteSequence bss = format.marshal(command);
        try {
            // We should get an exception from an invalid size value that is too large
            // Test OpenWireFormat#unmarshal(ByteSequence) method
            format.unmarshal(bss);
            fail("Should have received an IOException");
        } catch (IOException io) {
            assertTrue(io.getMessage().contains("Estimated allocated buffer size"));
            assertTrue(io.getMessage().contains("is larger than frame size"));
        }
        // Verify thread local is cleared even after exception
        assertNull(format.getMarshallingContext());

        try {
            // We should get an exception from an invalid size value that is too large
            // Test OpenWireFormat#unmarshal(DataInput) method
            format.unmarshal(new DataInputStream(new NIOInputStream(
                ByteBuffer.wrap(bss.toArray()))));
            fail("Should have received an IOException");
        } catch (IOException io) {
            assertTrue(io.getMessage().contains("Estimated allocated buffer size"));
            assertTrue(io.getMessage().contains("is larger than frame size"));
        }
        // Verify thread local is cleared even after exception
        assertNull(format.getMarshallingContext());
    }

    // Verify MarshallingContext thread local is cleared where there is
    // successful unmarshalling and no error. The other tests that check
    // validation works if invalid size will validate the context is cleared
    // when there is an error
    @Test
    public void testUnmarshalNoErrorClearContext() throws Exception {
        var format = new OpenWireFormat();
        ByteSequence bss = format.marshal(new ConnectionInfo());

        // make sure context cleared after calling
        // OpenWireFormat#unmarshal(ByteSequence) method
        format.unmarshal(bss);
        assertNull(format.getMarshallingContext());

        // Make sure context cleared after calling
        // OpenWireFormat#unmarshal(DataInput) method
        format.unmarshal(new DataInputStream(new NIOInputStream(
            ByteBuffer.wrap(bss.toArray()))));
        assertNull(format.getMarshallingContext());
    }

    @Test
    public void testOpenwireThrowableValidation() throws Exception {
        // Create a format which will use loose encoding by default
        // The code for handling exception creation is shared between both
        // tight/loose encoding so only need to test 1
        var format = setupWireFormat(false);

        // Build the response and try to unmarshal which should give an IllegalArgumentExeption on unmarshall
        // as the test marshaller should have encoded a class type that is not a Throwable
        ExceptionResponse r = new ExceptionResponse();
        r.setException(new Exception());
        ByteSequence bss = format.marshal(r);
        ExceptionResponse response = (ExceptionResponse) format.unmarshal(bss);

        assertTrue(response.getException() instanceof IllegalArgumentException);
        assertTrue(response.getException().getMessage().contains("is not assignable to Throwable"));

        // assert the class was never initialized
        assertFalse(initialized.get());
    }

    private OpenWireFormat setupWireFormat(boolean tightEncoding) throws Exception {
        // Create a format
        OpenWireFormat format = new OpenWireFormat();
        format.setTightEncodingEnabled(tightEncoding);

        // Override the marshaller map with a custom impl to purposely marshal a bad size value
        Class<?> marshallerFactory = getMarshallerFactory();
        Method createMarshallerMap = marshallerFactory.getMethod("createMarshallerMap", OpenWireFormat.class);
        DataStreamMarshaller[] map = (DataStreamMarshaller[]) createMarshallerMap.invoke(marshallerFactory, format);
        map[ExceptionResponse.DATA_STRUCTURE_TYPE] = getExceptionMarshaller();
        map[WireFormatInfo.DATA_STRUCTURE_TYPE] = getWireFormatInfoMarshaller();
        map[PartialCommand.DATA_STRUCTURE_TYPE] = getPartialCommandMarshaller();
        // This will trigger updating the marshaller from the marshaller map with the right version
        format.setVersion(version);
        return format;
    }

    static class NotAThrowable {
        private String message;

        static {
            // Class should not be initialized so set flag here to verify
            initialized.set(true);
        }

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

    // Create test marshallers for all non-legacy versions
    // WireFormatInfo will test the bytesequence marshallers
    protected DataStreamMarshaller getWireFormatInfoMarshaller() {
        switch (version) {
            case 12:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v12.WireFormatInfoMarshaller());
            case 11:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v11.WireFormatInfoMarshaller());
            case 10:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v10.WireFormatInfoMarshaller());
            case 9:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v9.WireFormatInfoMarshaller());
            case 1:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v1.WireFormatInfoMarshaller());
            default:
                throw new IllegalArgumentException("Unknown OpenWire version of " + version);
        }
    }

    // PartialCommand will test the byte array marshallers
    protected DataStreamMarshaller getPartialCommandMarshaller() {
        switch (version) {
            case 12:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v12.PartialCommandMarshaller());
            case 11:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v11.PartialCommandMarshaller());
            case 10:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v10.PartialCommandMarshaller());
            case 9:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v9.PartialCommandMarshaller());
            case 1:
                return proxyBadBufferCommand(new org.apache.activemq.openwire.v1.PartialCommandMarshaller());
            default:
                throw new IllegalArgumentException("Unknown OpenWire version of " + version);
        }
    }

    protected static void badTightMarshalByteSequence(ByteSequence data, DataOutput dataOut,
                                               BooleanStream bs) throws IOException {
        if (bs.readBoolean()) {
            // Write an invalid length that is much larger than it should be
            dataOut.writeInt(data.getLength() * 10);
            dataOut.write(data.getData(), data.getOffset(), data.getLength());
        }
    }

    protected static void badLooseMarshalByteSequence(ByteSequence data, DataOutput dataOut)
        throws IOException {
        dataOut.writeBoolean(data != null);
        if (data != null) {
            // Write an invalid length that is much larger than it should be
            dataOut.writeInt(data.getLength() * 10);
            dataOut.write(data.getData(), data.getOffset(), data.getLength());
        }
    }

    protected static void badLooseMarshalByteArray(byte[] data,
                                            DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(data != null);
        if (data != null) {
            // Write an invalid length that is much larger than it should be
            dataOut.writeInt(data.length * 10);
            dataOut.write(data);
        }
    }

    protected static void badTightMarshalByteArray(byte[] data, DataOutput dataOut,
                                            BooleanStream bs) throws IOException {
        if (bs.readBoolean()) {
            // Write an invalid length that is much larger than it should be
            dataOut.writeInt(data.length * 10);
            dataOut.write(data);
        }
    }

    // This will create a proxy object to wrap the marhallers so that we can intercept
    // both the byte and bytesequence methods to write bad sizes for testing
    protected DataStreamMarshaller proxyBadBufferCommand(DataStreamMarshaller marshaller) {
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(marshaller.getClass());
        Class<?> clazz = factory.createClass();

        try {
            DataStreamMarshaller instance = (DataStreamMarshaller) clazz.getConstructor().newInstance();
            ((ProxyObject) instance).setHandler(new BadBufferProxy());
            return instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static class BadBufferProxy implements MethodHandler {

        @Override
        public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
            Object result = null;

            try {
                // This handles writing a bad size for all 4 types of methods that should validate
                switch (thisMethod.getName()) {
                    case "looseMarshalByteArray":
                        badLooseMarshalByteArray((byte[]) args[1], (DataOutput) args[2]);
                        break;
                    case "tightMarshalByteArray2":
                        badTightMarshalByteArray((byte[]) args[0], (DataOutput) args[1], (BooleanStream) args[2]);
                        break;
                    case "looseMarshalByteSequence":
                        badLooseMarshalByteSequence((ByteSequence) args[1], (DataOutput) args[2]);
                        break;
                    case "tightMarshalByteSequence2":
                        badTightMarshalByteSequence((ByteSequence) args[0], (DataOutput) args[1], (BooleanStream) args[2]);
                        break;
                    default:
                        result = proceed.invoke(self, args);
                        break;
                }
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }

            return result;
        }
    }
}
