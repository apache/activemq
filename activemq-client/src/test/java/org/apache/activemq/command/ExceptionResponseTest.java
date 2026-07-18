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
package org.apache.activemq.command;

import static org.junit.Assert.*;

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;

import org.apache.activemq.ActiveMQErrorCode;
import org.junit.Test;

public class ExceptionResponseTest {

    @Test
    public void testNullErrorCodeLeavesExceptionUntouched() {
        JMSException original = new JMSException("boom");
        ExceptionResponse response = new ExceptionResponse(original);

        assertSame(original, response.getException());
        assertNull(response.getErrorCode());
    }

    @Test
    public void testErrorCodeAppliedToJMSException() {
        ExceptionResponse response = new ExceptionResponse(new JMSException("boom"));
        response.setErrorCode(ActiveMQErrorCode.SELECTOR_MISMATCH);

        Throwable result = response.getException();
        assertTrue(result instanceof JMSException);
        assertEquals(ActiveMQErrorCode.SELECTOR_MISMATCH, ((JMSException) result).getErrorCode());
        assertEquals("boom", result.getMessage());
    }

    @Test
    public void testErrorCodeAppliedToJMSExceptionSubclass() {
        ExceptionResponse response =
                new ExceptionResponse(new InvalidDestinationException("bad dest"));
        response.setErrorCode(ActiveMQErrorCode.INVALID_DESTINATION);

        Throwable result = response.getException();
        assertTrue("Subclass type must be preserved", result instanceof InvalidDestinationException);
        assertEquals(ActiveMQErrorCode.INVALID_DESTINATION,
                ((JMSException) result).getErrorCode());
    }

    @Test
    public void testMatchingErrorCodeDoesNotReconstruct() {
        JMSException original = new JMSException("boom", ActiveMQErrorCode.SELECTOR_MISMATCH);
        ExceptionResponse response = new ExceptionResponse(original);
        response.setErrorCode(ActiveMQErrorCode.SELECTOR_MISMATCH);

        assertSame("Already-correct errorCode must skip reflection", original,
                response.getException());
    }

    @Test
    public void testNonJMSExceptionIgnoresErrorCode() {
        RuntimeException original = new RuntimeException("boom");
        ExceptionResponse response = new ExceptionResponse(original);
        response.setErrorCode(ActiveMQErrorCode.SELECTOR_MISMATCH);

        assertSame(original, response.getException());
    }

    @Test
    public void testExceptionWithoutTwoArgConstructorFallsBackToOriginal() {
        JMSException original = new NoTwoArgCtorException("boom");
        ExceptionResponse response = new ExceptionResponse(original);
        response.setErrorCode(ActiveMQErrorCode.SELECTOR_MISMATCH);

        assertSame("Missing (String,String) ctor must leave the exception intact",
                original, response.getException());
    }

    @Test
    public void testReconstructionPreservesCauseAndLinkedException() {
        Throwable cause = new IllegalStateException("root cause");
        Exception linked = new Exception("linked");
        JMSException original = new JMSException("boom");
        original.initCause(cause);
        original.setLinkedException(linked);

        ExceptionResponse response = new ExceptionResponse(original);
        response.setErrorCode(ActiveMQErrorCode.SUBSCRIPTION_IN_USE);

        JMSException result = (JMSException) response.getException();
        assertEquals(ActiveMQErrorCode.SUBSCRIPTION_IN_USE, result.getErrorCode());
        assertSame(cause, result.getCause());
        assertSame(linked, result.getLinkedException());
        assertArrayEquals(original.getStackTrace(), result.getStackTrace());
    }

    @Test
    public void testGetExceptionIsIdempotent() {
        ExceptionResponse response = new ExceptionResponse(new JMSException("boom"));
        response.setErrorCode(ActiveMQErrorCode.SUBSCRIPTION_TYPE_CONFLICT);

        Throwable first = response.getException();
        Throwable second = response.getException();
        assertSame("Second call must not reconstruct again", first, second);
    }

    @Test
    public void testIsException() {
        assertTrue(new ExceptionResponse(new JMSException("boom")).isException());
    }

    @Test
    public void testDataStructureType() {
        assertEquals(CommandTypes.EXCEPTION_RESPONSE,
                new ExceptionResponse().getDataStructureType());
    }

    /** A JMSException subclass lacking the (String message, String errorCode) constructor. */
    private static class NoTwoArgCtorException extends JMSException {
        private static final long serialVersionUID = 1L;

        NoTwoArgCtorException(String reason) {
            super(reason);
        }
    }
}
