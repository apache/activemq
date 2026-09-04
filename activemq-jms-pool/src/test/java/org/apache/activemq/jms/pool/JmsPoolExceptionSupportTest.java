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
package org.apache.activemq.jms.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidClientIDRuntimeException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.MessageNotWriteableRuntimeException;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.ResourceAllocationRuntimeException;
import jakarta.jms.TransactionInProgressException;
import jakarta.jms.TransactionInProgressRuntimeException;
import jakarta.jms.TransactionRolledBackException;
import jakarta.jms.TransactionRolledBackRuntimeException;

import org.junit.Test;

public class JmsPoolExceptionSupportTest {

    @Test(timeout = 60000)
    public void testToRuntimeExceptionMapsAllSpecTypes() {
        assertMapped(new jakarta.jms.IllegalStateException("m", "c"), IllegalStateRuntimeException.class);
        assertMapped(new InvalidClientIDException("m", "c"), InvalidClientIDRuntimeException.class);
        assertMapped(new InvalidDestinationException("m", "c"), InvalidDestinationRuntimeException.class);
        assertMapped(new InvalidSelectorException("m", "c"), InvalidSelectorRuntimeException.class);
        assertMapped(new JMSSecurityException("m", "c"), JMSSecurityRuntimeException.class);
        assertMapped(new MessageFormatException("m", "c"), MessageFormatRuntimeException.class);
        assertMapped(new MessageNotWriteableException("m", "c"), MessageNotWriteableRuntimeException.class);
        assertMapped(new ResourceAllocationException("m", "c"), ResourceAllocationRuntimeException.class);
        assertMapped(new TransactionInProgressException("m", "c"), TransactionInProgressRuntimeException.class);
        assertMapped(new TransactionRolledBackException("m", "c"), TransactionRolledBackRuntimeException.class);

        // the base type and unrecognized subtypes map to the generic runtime exception
        assertMapped(new JMSException("m", "c"), JMSRuntimeException.class);
        assertMapped(new JMSException("m", "c") {}, JMSRuntimeException.class);
    }

    private static void assertMapped(JMSException cause, Class<? extends JMSRuntimeException> expectedType) {
        var mapped = JmsPoolExceptionSupport.toRuntimeException(cause);
        assertEquals("Wrong runtime exception type for " + cause.getClass().getSimpleName(),
            expectedType, mapped.getClass());
        assertEquals(cause.getMessage(), mapped.getMessage());
        assertEquals(cause.getErrorCode(), mapped.getErrorCode());
        assertSame(cause, mapped.getCause());
    }
}
