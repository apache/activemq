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

import java.text.DecimalFormat;

/**
 * Utility class to help displaying bytes in a human readable way.
 *
 */
public class SizeFormatterUtils {

    private static final DecimalFormat formatter = new DecimalFormat("#.###");

    private static String[] units = { "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
    private static String[] siUnits = { "KB", "MB", "GB", "TB", "PB", "EB" };

    /**
     * Format the bytes in a human readable way.  Supports both SI units (base 1000)
     * and base 1024. This method will round to 3 decimal places.
     * 
     * @param bytes the bytes to format
     * @param si true for SI, else false
     * @return
     */
    public static String humanReadableBytes(long bytes, boolean si) {
        for (int i = units.length - 1; i >= 0; i--) {
            double unit = Math.pow(si ? 1000 : 1024, i + 1);
            if (bytes >= unit) {
                return formatter.format(bytes / unit) + " " + (si ? siUnits[i] : units[i]);
            }
        }
        return bytes + " B";
    }

    /**
     * Formats the bytes in a human readable way using base 1024. This method will round to 3 decimal places.
     * 
     * @param bytes the bytes to format
     * @return
     */
    public static String humanReadableBytes(long bytes) {
        return humanReadableBytes(bytes, false);
    }
}
