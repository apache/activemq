/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.xnet.hba;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.StringTokenizer;
import java.net.InetAddress;
import java.net.Inet6Address;

/**
 * @version $Revision$ $Date$
 */
public class ExactIPv6AddressPermission implements IPAddressPermission {
    private static final Pattern MASK_VALIDATOR = Pattern.compile("^(([a-fA-F0-9]{1,4}:){7})([a-fA-F0-9]{1,4})$");

    public static boolean canSupport(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        return matcher.matches();
    }

    private final byte[] bytes;

    public ExactIPv6AddressPermission(byte[] bytes) {
        this.bytes = bytes;
    }

    public ExactIPv6AddressPermission(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        if (false == matcher.matches()) {
            throw new IllegalArgumentException("Mask " + mask + " does not match pattern " + MASK_VALIDATOR.pattern());
        }

        bytes = new byte[16];
        int pos = 0;
        StringTokenizer tokenizer = new StringTokenizer(mask, ":");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            int value = Integer.parseInt(token, 16);
            bytes[pos++] = (byte) ((value & 0xff00) >> 8);
            bytes[pos++] = (byte) value;
        }
    }

    public boolean implies(InetAddress address) {
        if (false == address instanceof Inet6Address) {
            return false;
        }

        byte[] byteAddress = address.getAddress();
        for (int i = 0; i < 16; i++) {
            if (byteAddress[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }
}
