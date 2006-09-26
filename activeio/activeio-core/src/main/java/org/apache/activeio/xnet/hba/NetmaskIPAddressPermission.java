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
public class NetmaskIPAddressPermission implements IPAddressPermission {
    private static final Pattern MASK_VALIDATOR = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})/((\\d{1,2})|(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3}))$");

    public static boolean canSupport(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        return matcher.matches();
    }

    private final byte[] networkAddressBytes;
    private final byte[] netmaskBytes;

    public NetmaskIPAddressPermission(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        if (false == matcher.matches()) {
            throw new IllegalArgumentException("Mask " + mask + " does not match pattern " + MASK_VALIDATOR.pattern());
        }

        networkAddressBytes = new byte[4];
        for (int i = 0; i < 4; i++) {
            String group = matcher.group(i + 1);
            int value = Integer.parseInt(group);
            if (value < 0 || 255 < value) {
                throw new IllegalArgumentException("byte #" + i + " is not valid.");
            }
            networkAddressBytes[i] = (byte) value;
        }

        netmaskBytes = new byte[4];
        String netmask = matcher.group(6);
        if (null != netmask) {
            int value = Integer.parseInt(netmask);
            int pos = value / 8;
            int shift = 8 - value % 8;
            for (int i = 0; i < pos; i++) {
                netmaskBytes[i] = (byte) 0xff;
            }
            netmaskBytes[pos] = (byte) (0xff << shift);
        } else {
            for (int i = 0; i < 4; i++) {
                String group = matcher.group(i + 7);
                int value = Integer.parseInt(group);
                if (value < 0 || 255 < value) {
                    throw new IllegalArgumentException("byte #" + i + " is not valid.");
                }
                netmaskBytes[i] = (byte) value;
            }
        }
    }

    public boolean implies(InetAddress address) {
        if (false == address instanceof Inet4Address) {
            return false;
        }

        byte[] byteAddress = address.getAddress();
        for (int i = 0; i < 4; i++) {
            if ((netmaskBytes[i] & byteAddress[i]) != networkAddressBytes[i]) {
                return false;
            }
        }
        return true;
    }
}
