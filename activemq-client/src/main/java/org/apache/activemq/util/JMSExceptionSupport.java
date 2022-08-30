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

import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidClientIDRuntimeException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.InvalidSelectorRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageNotWriteableRuntimeException;
import javax.jms.ResourceAllocationRuntimeException;
import javax.jms.TransactionInProgressRuntimeException;
import javax.jms.TransactionRolledBackRuntimeException;

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
        if (e instanceof javax.jms.IllegalStateException) {
            return new IllegalStateRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.InvalidClientIDException) {
            return new InvalidClientIDRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.InvalidDestinationException) {
            return new InvalidDestinationRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.InvalidSelectorException) {
            return new InvalidSelectorRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.JMSSecurityException) {
            return new JMSSecurityRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.MessageFormatException) {
            return new MessageFormatRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.MessageNotWriteableException) {
            return new MessageNotWriteableRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.ResourceAllocationException) {
            return new ResourceAllocationRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.TransactionInProgressException) {
            return new TransactionInProgressRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        if (e instanceof javax.jms.TransactionRolledBackException) {
            return new TransactionRolledBackRuntimeException(e.getMessage(), e.getErrorCode(), e);
        }
        return new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
    }
}
