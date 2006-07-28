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
import java.net.Inet4Address;

/**
 * @version $Revision$ $Date$
 */
public class FactorizedIPAddressPermission implements IPAddressPermission {
    private static final Pattern MASK_VALIDATOR = Pattern.compile("^((\\d{1,3}){1}(\\.\\d{1,3}){0,2}\\.)?\\{(\\d{1,3}){1}((,\\d{1,3})*)\\}$");

    public static boolean canSupport(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        return matcher.matches();
    }

    private final byte[] prefixBytes;
    private final byte[] suffixBytes;

    public FactorizedIPAddressPermission(String mask) {
        Matcher matcher = MASK_VALIDATOR.matcher(mask);
        if (false == matcher.matches()) {
            throw new IllegalArgumentException("Mask " + mask + " does not match pattern " + MASK_VALIDATOR.pattern());
        }

        // group 1 is the factorized IP part.
        // e.g. group 1 in "1.2.3.{4,5,6}" is "1.2.3."
        String prefix = matcher.group(1);
        StringTokenizer tokenizer = new StringTokenizer(prefix, ".");
        prefixBytes = new byte[tokenizer.countTokens()];
        for (int i = 0; i < prefixBytes.length; i++) {
            String token = tokenizer.nextToken();
            int value = Integer.parseInt(token);
            if (value < 0 || 255 < value) {
                throw new IllegalArgumentException("byte #" + i + " is not valid.");
            }
            prefixBytes[i] = (byte) value;
        }

        // group 5 is a comma separated list of optional suffixes.
        // e.g. group 5 in "1.2.3.{4,5,6}" is ",5,6"
        String suffix = matcher.group(5);
        tokenizer = new StringTokenizer(suffix, ",");
        suffixBytes = new byte[1 + tokenizer.countTokens()];

        // group 4 is the compulsory and first suffix.
        // e.g. group 4 in "1.2.3.{4,5,6}" is "4"
        int value = Integer.parseInt(matcher.group(4));
        int i = 0;
        if (value < 0 || 255 < value) {
            throw new IllegalArgumentException("suffix " + i + " is not valid.");
        }
        suffixBytes[i++] = (byte) value;

        for (; i < suffixBytes.length; i++) {
            String token = tokenizer.nextToken();
            value = Integer.parseInt(token);
            if (value < 0 || 255 < value) {
                throw new IllegalArgumentException("byte #" + i + " is not valid.");
            }
            suffixBytes[i] = (byte) value;
        }
    }

    public boolean implies(InetAddress address) {
        if (false == address instanceof Inet4Address) {
            return false;
        }

        byte[] byteAddress = address.getAddress();
        for (int i = 0; i < prefixBytes.length; i++) {
            if (byteAddress[i] != prefixBytes[i]) {
                return false;
            }
        }
        byte lastByte = byteAddress[prefixBytes.length];
        for (int i = 0; i < suffixBytes.length; i++) {
            if (lastByte == suffixBytes[i]) {
                return true;
            }
        }
        return false;
    }
}
