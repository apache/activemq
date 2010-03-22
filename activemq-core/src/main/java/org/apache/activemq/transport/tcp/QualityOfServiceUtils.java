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
package org.apache.activemq.transport.tcp;

import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for determining the values for the bits in the headers of the
 * outgoing TCP/IP packets that indicate Traffic Class for use in Quality of
 * Service forwarding policies.
 */
public class QualityOfServiceUtils {

    private static final int MAX_DIFF_SERV = 63;
    private static final int MIN_DIFF_SERV = 0;
    private static final Map<String, Integer> DIFF_SERV_NAMES
        = new HashMap<String, Integer>();
    // TODO: Find other names used for Differentiated Services values.
    static {
        DIFF_SERV_NAMES.put("EF", 46);
        DIFF_SERV_NAMES.put("AF11", 10);
        DIFF_SERV_NAMES.put("AF12", 12);
        DIFF_SERV_NAMES.put("AF13", 14);
        DIFF_SERV_NAMES.put("AF21", 18);
        DIFF_SERV_NAMES.put("AF22", 20);
        DIFF_SERV_NAMES.put("AF23", 22);
        DIFF_SERV_NAMES.put("AF31", 26);
        DIFF_SERV_NAMES.put("AF32", 28);
        DIFF_SERV_NAMES.put("AF33", 30);
        DIFF_SERV_NAMES.put("AF41", 34);
        DIFF_SERV_NAMES.put("AF42", 36);
        DIFF_SERV_NAMES.put("AF43", 38);
    }

    private static final int MAX_TOS = 255;
    private static final int MIN_TOS = 0;

    /**
     * @param value A potential value to be used for Differentiated Services.
     * @return The corresponding Differentiated Services Code Point (DSCP).
     * @throws IllegalArgumentException if the value does not correspond to a
     *         Differentiated Services Code Point or setting the DSCP is not
     *         supported.
     */
    public static int getDSCP(String value) throws IllegalArgumentException {
        int intValue = -1;

        // Check the names first.
        if (DIFF_SERV_NAMES.containsKey(value)) {
            intValue = DIFF_SERV_NAMES.get(value);
        } else {
            try {
                intValue = Integer.parseInt(value);
                if (intValue > MAX_DIFF_SERV || intValue < MIN_DIFF_SERV) {
                    throw new IllegalArgumentException("Differentiated Services"
                        + " value: " + intValue + " not in legal range ["
                        + MIN_DIFF_SERV + ", " + MAX_DIFF_SERV + "].");
                }
            } catch (NumberFormatException e) {
                // value must have been a malformed name.
                throw new IllegalArgumentException("No such Differentiated "
                    + "Services name: " + value);
            }
        }

        return adjustDSCPForECN(intValue);
     }


    /**
     * @param value A potential value to be used for Type of Service.
     * @return A valid value that can be used to set the Type of Service in the
     *         packet headers.
     * @throws IllegalArgumentException if the value is not a legal Type of
     *         Service value.
     */
    public static int getToS(int value) throws IllegalArgumentException {
        if (value > MAX_TOS || value < MIN_TOS) {
            throw new IllegalArgumentException("Type of Service value: "
                + value + " not in legal range [" + MIN_TOS + ", " + MAX_TOS
                + ".");
        }
        return value;
    }

    /**
     * The Differentiated Services values use only 6 of the 8 bits in the field
     * in the TCP/IP packet header. Make sure any values the system has set for
     * the other two bits (the ECN bits) are maintained.
     *
     * @param dscp The Differentiated Services Code Point.
     * @return A Differentiated Services Code Point that respects the ECN bits
     *         set on the system.
     * @throws IllegalArgumentException if setting Differentiated Services is
     *         not supported.
     */
    private static int adjustDSCPForECN(int dscp)
            throws IllegalArgumentException {
        // The only way to see if there are any values set for the ECN is to
        // read the traffic class automatically set by the system and isolate
        // the ECN bits.
        Socket socket = new Socket();
        try {
            int systemTrafficClass = socket.getTrafficClass();
            // The 7th and 8th bits of the system traffic class are the ECN bits.
            return dscp | (systemTrafficClass & 192);
        } catch (SocketException e) {
            throw new IllegalArgumentException("Setting Differentiated Services"
                + " not supported: " + e);
        }
    }
}
