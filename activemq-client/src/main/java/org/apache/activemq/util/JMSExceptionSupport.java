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
package org.apache.activemq.util;

import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidClientIDRuntimeException;
import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageNotWriteableRuntimeException;
import jakarta.jms.ResourceAllocationRuntimeException;
import jakarta.jms.TransactionInProgressRuntimeException;
import jakarta.jms.TransactionRolledBackRuntimeException;

import org.apache.activemq.MaxFrameSizeExceededException;

public final class JMSExceptionSupport {

    private JMSExceptionSupport() {
    }

    public static JMSException create(String msg, Throwable cause) {
        JMSException exception = new JMSException(msg);
        exception.initCause(cause);
        return exception;
    }

    public static JMSException create(String msg, Exception cause) {
        JMSException exception = new JMSException(msg);
        exception.setLinkedException(cause);
        exception.initCause(cause);
        return exception;
    }

    public static JMSException create(Throwable cause) {
        if (cause instanceof JMSException) {
            return (JMSException)cause;
        }
        String msg = cause.getMessage();
        if (msg == null || msg.length() == 0) {
            msg = cause.toString();
        }
        JMSException exception;
        if (cause instanceof SecurityException) {
            exception = new JMSSecurityException(msg);
        } else {
            exception = new JMSException(msg);
        }
        exception.initCause(cause);
        return exception;
    }

    public static JMSException create(Exception cause) {
        if (cause instanceof JMSException) {
            return (JMSException)cause;
        }
        if (cause instanceof MaxFrameSizeExceededException) {
            JMSException jmsException = new JMSException(cause.getMessage(), "41300");
            jmsException.setLinkedException(cause);
            jmsException.initCause(cause);
            return jmsException;
        }
        String msg = cause.getMessage();
        if (msg == null || msg.length() == 0) {
            msg = cause.toString();
        }
        JMSException exception;
        if (cause instanceof SecurityException) {
            exception = new JMSSecurityException(msg);
        } else {
            exception = new JMSException(msg);
        }
        exception.setLinkedException(cause);
        exception.initCause(cause);
        return exception;
    }

    public static MessageEOFException createMessageEOFException(Exception cause) {
        String msg = cause.getMessage();
        if (msg == null || msg.length() == 0) {
            msg = cause.toString();
        }
        MessageEOFException exception = new MessageEOFException(msg);
        exception.setLinkedException(cause);
        exception.initCause(cause);
        return exception;
    }

    public static MessageFormatException createMessageFormatException(Exception cause) {
        String msg = cause.getMessage();
        if (msg == null || msg.length() == 0) {
            msg = cause.toString();
        }
        MessageFormatException exception = new MessageFormatException(msg);
        exception.setLinkedException(cause);
        exception.initCause(cause);
        return exception;
    }

    public static JMSRuntimeException convertToJMSRuntimeException(JMSException e) {
        if (e instanceof jakarta.jms.IllegalStateException) {
            return new IllegalStateRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.InvalidClientIDException) {
            return new InvalidClientIDRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.InvalidDestinationException) {
            return new InvalidDestinationRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.InvalidSelectorException) {
            return new InvalidSelectorRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.JMSSecurityException) {
            return new JMSSecurityRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.MessageFormatException) {
            return new MessageFormatRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.MessageNotWriteableException) {
            return new MessageNotWriteableRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.ResourceAllocationException) {
            return new ResourceAllocationRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.TransactionInProgressException) {
            return new TransactionInProgressRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof jakarta.jms.TransactionRolledBackException) {
            return new TransactionRolledBackRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        return new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
    }
}
