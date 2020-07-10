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

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts string values like "20 Mb", "1024kb", and "1g" to long or int values in bytes.
 */
public final class XBeanByteConverterUtil {

    private static final Pattern[] BYTE_MATCHERS = new Pattern[] {
            Pattern.compile("^\\s*(\\d+)\\s*(b)?\\s*$", Pattern.CASE_INSENSITIVE),
            Pattern.compile("^\\s*(\\d+)\\s*k(b)?\\s*$", Pattern.CASE_INSENSITIVE),
            Pattern.compile("^\\s*(\\d+)\\s*m(b)?\\s*$", Pattern.CASE_INSENSITIVE),
            Pattern.compile("^\\s*(\\d+)\\s*g(b)?\\s*$", Pattern.CASE_INSENSITIVE)};

    private XBeanByteConverterUtil() {
        // complete
    }

    public static Long convertToLongBytes(String str) throws IllegalArgumentException {
        for (int i = 0; i < BYTE_MATCHERS.length; i++) {
            Matcher matcher = BYTE_MATCHERS[i].matcher(str);
            if (matcher.matches()) {
                long value = Long.parseLong(matcher.group(1));
                for (int j = 1; j <= i; j++) {
                    value *= 1024;
                }
                return Long.valueOf(value);
            }
        }
        throw new IllegalArgumentException("Could not convert to a memory size: " + str);
    }

    public static Integer convertToIntegerBytes(String str) throws IllegalArgumentException {
        for (int i = 0; i < BYTE_MATCHERS.length; i++) {
            Matcher matcher = BYTE_MATCHERS[i].matcher(str);
            if (matcher.matches()) {
                int value = Integer.parseInt(matcher.group(1));
                for (int j = 1; j <= i; j++) {
                    value *= 1024;
                }
                return Integer.valueOf(value);
            }
        }
        throw new IllegalArgumentException("Could not convert to a memory size: " + str);
    }
    
}
