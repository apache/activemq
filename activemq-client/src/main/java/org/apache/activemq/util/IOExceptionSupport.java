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

import java.io.IOException;
import java.math.BigInteger;

import org.apache.activemq.MaxFrameSizeExceededException;

public final class IOExceptionSupport {

    private IOExceptionSupport() {
    }

    public static IOException create(String msg, Throwable cause) {
        IOException exception = new IOException(msg);
        exception.initCause(cause);
        return exception;
    }

    public static IOException create(String msg, Exception cause) {
        IOException exception = new IOException(msg);
        exception.initCause(cause);
        return exception;
    }

    public static IOException create(Throwable cause) {
        IOException exception = new IOException(cause.getMessage());
        exception.initCause(cause);
        return exception;
    }

    public static IOException create(Exception cause) {
        IOException exception = new IOException(cause.getMessage());
        exception.initCause(cause);
        return exception;
    }

    public static IOException createFrameSizeException(int size, long maxSize) {
        return new MaxFrameSizeExceededException("Frame size of " + toHumanReadableSizeString(size) +
            " larger than max allowed " + toHumanReadableSizeString(maxSize));
    }

    private static String toHumanReadableSizeString(final int size) {
        return toHumanReadableSizeString(BigInteger.valueOf(size));
    }

    private static String toHumanReadableSizeString(final long size) {
        return toHumanReadableSizeString(BigInteger.valueOf(size));
    }

    private static String toHumanReadableSizeString(final BigInteger size) {
        String displaySize;

        final BigInteger ONE_KB_BI = BigInteger.valueOf(1024);
        final BigInteger ONE_MB_BI = ONE_KB_BI.multiply(ONE_KB_BI);
        final BigInteger ONE_GB_BI = ONE_KB_BI.multiply(ONE_MB_BI);

        if (size.divide(ONE_GB_BI).compareTo(BigInteger.ZERO) > 0) {
            displaySize = String.valueOf(size.divide(ONE_GB_BI)) + " GB";
        } else if (size.divide(ONE_MB_BI).compareTo(BigInteger.ZERO) > 0) {
            displaySize = String.valueOf(size.divide(ONE_MB_BI)) + " MB";
        } else if (size.divide(ONE_KB_BI).compareTo(BigInteger.ZERO) > 0) {
            displaySize = String.valueOf(size.divide(ONE_KB_BI)) + " KB";
        } else {
            displaySize = String.valueOf(size) + " bytes";
        }

        return displaySize;
    }
}
