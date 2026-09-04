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

/**
 * Converts checked {@link JMSException} types into their unchecked JMS 2.0
 * counterparts, preserving the specific runtime exception subtype the
 * specification defines for each checked type.
 *
 * activemq-jms-pool depends only on the jakarta.jms API to pool any JMS
 * provider. No re-use of activemq-client JMSExceptionSupport.
 */
final class JmsPoolExceptionSupport {

    private JmsPoolExceptionSupport() {}

    static JMSRuntimeException toRuntimeException(JMSException e) {
        var message = e.getMessage();
        var errorCode = e.getErrorCode();
        if (e instanceof jakarta.jms.IllegalStateException) {
            return new IllegalStateRuntimeException(message, errorCode, e);
        }
        if (e instanceof InvalidClientIDException) {
            return new InvalidClientIDRuntimeException(message, errorCode, e);
        }
        if (e instanceof InvalidDestinationException) {
            return new InvalidDestinationRuntimeException(message, errorCode, e);
        }
        if (e instanceof InvalidSelectorException) {
            return new InvalidSelectorRuntimeException(message, errorCode, e);
        }
        if (e instanceof JMSSecurityException) {
            return new JMSSecurityRuntimeException(message, errorCode, e);
        }
        if (e instanceof MessageFormatException) {
            return new MessageFormatRuntimeException(message, errorCode, e);
        }
        if (e instanceof MessageNotWriteableException) {
            return new MessageNotWriteableRuntimeException(message, errorCode, e);
        }
        if (e instanceof ResourceAllocationException) {
            return new ResourceAllocationRuntimeException(message, errorCode, e);
        }
        if (e instanceof TransactionInProgressException) {
            return new TransactionInProgressRuntimeException(message, errorCode, e);
        }
        if (e instanceof TransactionRolledBackException) {
            return new TransactionRolledBackRuntimeException(message, errorCode, e);
        }
        return new JMSRuntimeException(message, errorCode, e);
    }
}
