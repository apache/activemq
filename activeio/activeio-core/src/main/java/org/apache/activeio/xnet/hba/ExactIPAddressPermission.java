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
import java.net.InetAddress;
import java.net.Inet4Address;

/**
 * @version $Revision$ $Date$
 */
public class ExactIPAddressPermission implements IPAddressPermission {
    private static final Pattern MASK_VALIDATOR = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

    public static boolean canSupport(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        return matcher.matches();
    }

    private final byte[] bytes;

    public ExactIPAddressPermission(byte[] bytes) {
        this.bytes = bytes;
    }

    public ExactIPAddressPermission(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        if (false == matcher.matches()) {
            throw new IllegalArgumentException("Mask " + mask + " does not match pattern " + MASK_VALIDATOR.pattern());
        }

        bytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            String group = matcher.group(i + 1);
            int value = Integer.parseInt(group);
            if (value < 0 || 255 < value) {
                throw new IllegalArgumentException("byte #" + i + " is not valid.");
            }
            bytes[i] = (byte) value;
        }
    }

    public boolean implies(InetAddress address) {
        if (false == address instanceof Inet4Address) {
            return false;
        }

        byte[] byteAddress = address.getAddress();
        for (int i = 0; i < 4; i++) {
            if (byteAddress[i] != bytes[i]) {
                return false;
            }
        }
        return true;
    }
}
