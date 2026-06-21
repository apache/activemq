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

import jakarta.jms.JMSException;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageFormatRuntimeException;

import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.ActiveMQMessageFormatException;

public class ExceptionUtils {


    /**
     * Creates a new ActiveMQMessageFormatException by wrapping the existing throwable.
     * This will only wrap the exception if the throwable contains a message format
     * error as the root cause
     *
     * @param error original exception
     * @return ActiveMQMessageFormatException if a message format error, else null
     */
    public static ActiveMQMessageFormatException createMessageFormatException(Throwable error) {
        if (error instanceof ActiveMQMessageFormatException) {
            return (ActiveMQMessageFormatException) error;
        } else if (containsMessageFormatError(error)) {
            return new ActiveMQMessageFormatException(error);
        }
        return null;
    }

    /*
     * Check if this throwable contains a message format error.
     * This will check the root cause and any linked exceptions as well
     * if the exception is a JMSException
     */
    private static boolean containsMessageFormatError(Throwable error) {
        if (error == null) {
            return false;
        }

        Throwable cause = ExceptionUtils.getRootCause(error);
        return isMessageFormatError(cause) ||
                error instanceof JMSException && isMessageFormatError(((JMSException) error).getLinkedException());
    }

    /*
     * Checks if the error is considered an error with the format of the message.
     * This checks for the ActiveMQ custom ActiveMQUnmarshalException that
     * can be thrown by MarshallingSupport as well as JMS specific exceptions
     * that indicated corruption/read problems such as MessageFormatException
     * and MessageEOFException.
     */
    private static boolean isMessageFormatError(Throwable error) {
        if (error == null) {
            return false;
        }

        return error instanceof MarshallingSupport.ActiveMQUnmarshalException ||
                error instanceof MessageFormatException ||
                error instanceof MessageEOFException;
    }

    public static Throwable getRootCause(final Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        final List<Throwable> list = getThrowableList(throwable);
        return list.isEmpty() ? null : list.get(list.size() - 1);
    }

    static List<Throwable> getThrowableList(Throwable throwable) {
        final List<Throwable> list = new ArrayList<>();
        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = throwable.getCause();
        }
        return list;
    }
}
