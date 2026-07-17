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
package org.apache.activemq.openwire.v13;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import jakarta.jms.JMSException;

import org.apache.activemq.ActiveMQErrorCode;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.openwire.OpenWireFormat;
import org.junit.Test;

/**
 * Round-trips the v13 {@code errorCode} field on {@link ExceptionResponse}
 * in both tight and loose encodings.
 */
public class ExceptionResponseMarshallerRoundTripTest {

    @Test
    public void testErrorCodeSurvivesTightRoundTrip() throws Exception {
        assertErrorCodeRoundTrip(true);
    }

    @Test
    public void testErrorCodeSurvivesLooseRoundTrip() throws Exception {
        assertErrorCodeRoundTrip(false);
    }

    @Test
    public void testNullErrorCodeSurvivesRoundTrip() throws Exception {
        OpenWireFormat wireFormat = wireFormat(true);
        ExceptionResponse original = new ExceptionResponse(new JMSException("boom"));

        ExceptionResponse restored =
                (ExceptionResponse) unmarshal(wireFormat, marshal(wireFormat, original));

        assertNull(restored.getErrorCode());
    }

    /**
     * OpenWire marshals a throwable as type plus message only, dropping
     * {@code JMSException.errorCode}. The separate v13 errorCode field is what
     * carries it across, and {@code getException()} reapplies it on the far side.
     */
    @Test
    public void testErrorCodeReappliedToExceptionAfterUnmarshal() throws Exception {
        OpenWireFormat wireFormat = wireFormat(true);

        ExceptionResponse original = new ExceptionResponse(new JMSException("boom"));
        original.setErrorCode(ActiveMQErrorCode.SUBSCRIPTION_ALREADY_EXISTS);

        ExceptionResponse restored =
                (ExceptionResponse) unmarshal(wireFormat, marshal(wireFormat, original));

        Throwable exception = restored.getException();
        assertTrue(exception instanceof JMSException);
        assertEquals(ActiveMQErrorCode.SUBSCRIPTION_ALREADY_EXISTS,
                ((JMSException) exception).getErrorCode());
        assertEquals("boom", exception.getMessage());
    }

    /**
     * The v13 errorCode must not reach the v12 wire, leaving a v12 peer with
     * exactly the exception it would have received before this field existed.
     */
    @Test
    public void testErrorCodeIsNotWrittenAtV12() throws Exception {
        OpenWireFormat wireFormat = wireFormat(true);
        wireFormat.setVersion(12);

        ExceptionResponse original = new ExceptionResponse(new JMSException("boom"));
        original.setErrorCode(ActiveMQErrorCode.SELECTOR_MISMATCH);

        ExceptionResponse restored =
                (ExceptionResponse) unmarshal(wireFormat, marshal(wireFormat, original));

        assertNull("errorCode must not cross the v12 wire", restored.getErrorCode());
        assertTrue(restored.getException() instanceof JMSException);
        assertEquals("boom", restored.getException().getMessage());
    }

    private void assertErrorCodeRoundTrip(boolean tightEncoding) throws Exception {
        OpenWireFormat wireFormat = wireFormat(tightEncoding);

        ExceptionResponse original = new ExceptionResponse(new JMSException("boom"));
        original.setErrorCode(ActiveMQErrorCode.SELECTOR_MISMATCH);

        ExceptionResponse restored =
                (ExceptionResponse) unmarshal(wireFormat, marshal(wireFormat, original));

        assertEquals(ActiveMQErrorCode.SELECTOR_MISMATCH, restored.getErrorCode());
        assertTrue(restored.isException());
    }

    private static OpenWireFormat wireFormat(boolean tightEncoding) {
        OpenWireFormat wireFormat = new OpenWireFormat();
        wireFormat.setVersion(13);
        wireFormat.setCacheEnabled(false);
        wireFormat.setTightEncodingEnabled(tightEncoding);
        return wireFormat;
    }

    private static byte[] marshal(OpenWireFormat wireFormat, Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        wireFormat.marshal(obj, out);
        out.flush();
        return baos.toByteArray();
    }

    private static Object unmarshal(OpenWireFormat wireFormat, byte[] bytes) throws Exception {
        return wireFormat.unmarshal(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
}
