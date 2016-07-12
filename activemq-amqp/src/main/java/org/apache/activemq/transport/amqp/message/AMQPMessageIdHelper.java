/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.activemq.transport.amqp.message;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedLong;

/**
 * Helper class for identifying and converting message-id and correlation-id values between
 * the AMQP types and the Strings values used by JMS.
 *
 * <p>AMQP messages allow for 4 types of message-id/correlation-id: message-id-string, message-id-binary,
 * message-id-uuid, or message-id-ulong. In order to accept or return a string representation of these
 * for interoperability with other AMQP clients, the following encoding can be used after removing or
 * before adding the "ID:" prefix used for a JMSMessageID value:<br>
 *
 * {@literal "AMQP_BINARY:<hex representation of binary content>"}<br>
 * {@literal "AMQP_UUID:<string representation of uuid>"}<br>
 * {@literal "AMQP_ULONG:<string representation of ulong>"}<br>
 * {@literal "AMQP_STRING:<string>"}<br>
 *
 * <p>The AMQP_STRING encoding exists only for escaping message-id-string values that happen to begin
 * with one of the encoding prefixes (including AMQP_STRING itself). It MUST NOT be used otherwise.
 *
 * <p>When provided a string for conversion which attempts to identify itself as an encoded binary, uuid, or
 * ulong but can't be converted into the indicated format, an exception will be thrown.
 */
public class AMQPMessageIdHelper {

    public static final AMQPMessageIdHelper INSTANCE = new AMQPMessageIdHelper();

    public static final String AMQP_STRING_PREFIX = "AMQP_STRING:";
    public static final String AMQP_UUID_PREFIX = "AMQP_UUID:";
    public static final String AMQP_ULONG_PREFIX = "AMQP_ULONG:";
    public static final String AMQP_BINARY_PREFIX = "AMQP_BINARY:";

    private static final int AMQP_UUID_PREFIX_LENGTH = AMQP_UUID_PREFIX.length();
    private static final int AMQP_ULONG_PREFIX_LENGTH = AMQP_ULONG_PREFIX.length();
    private static final int AMQP_STRING_PREFIX_LENGTH = AMQP_STRING_PREFIX.length();
    private static final int AMQP_BINARY_PREFIX_LENGTH = AMQP_BINARY_PREFIX.length();
    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    /**
     * Takes the provided AMQP messageId style object, and convert it to a base string.
     * Encodes type information as a prefix where necessary to convey or escape the type
     * of the provided object.
     *
     * @param messageId
     *      the raw messageId object to process
     *
     * @return the base string to be used in creating the actual id.
     */
    public String toBaseMessageIdString(Object messageId) {
        if (messageId == null) {
            return null;
        } else if (messageId instanceof String) {
            String stringId = (String) messageId;

            // If the given string has a type encoding prefix,
            // we need to escape it as an encoded string (even if
            // the existing encoding prefix was also for string)
            if (hasTypeEncodingPrefix(stringId)) {
                return AMQP_STRING_PREFIX + stringId;
            } else {
                return stringId;
            }
        } else if (messageId instanceof UUID) {
            return AMQP_UUID_PREFIX + messageId.toString();
        } else if (messageId instanceof UnsignedLong) {
            return AMQP_ULONG_PREFIX + messageId.toString();
        } else if (messageId instanceof Binary) {
            ByteBuffer dup = ((Binary) messageId).asByteBuffer();

            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);

            String hex = convertBinaryToHexString(bytes);

            return AMQP_BINARY_PREFIX + hex;
        } else {
            throw new IllegalArgumentException("Unsupported type provided: " + messageId.getClass());
        }
    }

    /**
     * Takes the provided base id string and return the appropriate amqp messageId style object.
     * Converts the type based on any relevant encoding information found as a prefix.
     *
     * @param baseId
     *      the object to be converted to an AMQP MessageId value.
     *
     * @return the AMQP messageId style object
     *
     * @throws AmqpProtocolException if the provided baseId String indicates an encoded type but can't be converted to that type.
     */
    public Object toIdObject(String baseId) throws AmqpProtocolException {
        if (baseId == null) {
            return null;
        }

        try {
            if (hasAmqpUuidPrefix(baseId)) {
                String uuidString = strip(baseId, AMQP_UUID_PREFIX_LENGTH);
                return UUID.fromString(uuidString);
            } else if (hasAmqpUlongPrefix(baseId)) {
                String longString = strip(baseId, AMQP_ULONG_PREFIX_LENGTH);
                return UnsignedLong.valueOf(longString);
            } else if (hasAmqpStringPrefix(baseId)) {
                return strip(baseId, AMQP_STRING_PREFIX_LENGTH);
            } else if (hasAmqpBinaryPrefix(baseId)) {
                String hexString = strip(baseId, AMQP_BINARY_PREFIX_LENGTH);
                byte[] bytes = convertHexStringToBinary(hexString);
                return new Binary(bytes);
            } else {
                // We have a string without any type prefix, transmit it as-is.
                return baseId;
            }
        } catch (IllegalArgumentException e) {
            throw new AmqpProtocolException("Unable to convert ID value");
        }
    }

    /**
     * Convert the provided hex-string into a binary representation where each byte represents
     * two characters of the hex string.
     *
     * The hex characters may be upper or lower case.
     *
     * @param hexString
     *      string to convert to a binary value.
     *
     * @return a byte array containing the binary representation
     *
     * @throws IllegalArgumentException if the provided String is a non-even length or contains
     *                                  non-hex characters
     */
    public byte[] convertHexStringToBinary(String hexString) throws IllegalArgumentException {
        int length = hexString.length();

        // As each byte needs two characters in the hex encoding, the string must be an even length.
        if (length % 2 != 0) {
            throw new IllegalArgumentException("The provided hex String must be an even length, but was of length " + length + ": " + hexString);
        }

        byte[] binary = new byte[length / 2];

        for (int i = 0; i < length; i += 2) {
            char highBitsChar = hexString.charAt(i);
            char lowBitsChar = hexString.charAt(i + 1);

            int highBits = hexCharToInt(highBitsChar, hexString) << 4;
            int lowBits = hexCharToInt(lowBitsChar, hexString);

            binary[i / 2] = (byte) (highBits + lowBits);
        }

        return binary;
    }

    /**
     * Convert the provided binary into a hex-string representation where each character
     * represents 4 bits of the provided binary, i.e each byte requires two characters.
     *
     * The returned hex characters are upper-case.
     *
     * @param bytes
     *      the binary value to convert to a hex String instance.
     *
     * @return a String containing a hex representation of the bytes
     */
    public String convertBinaryToHexString(byte[] bytes) {
        // Each byte is represented as 2 chars
        StringBuilder builder = new StringBuilder(bytes.length * 2);

        for (byte b : bytes) {
            // The byte will be expanded to int before shifting, replicating the
            // sign bit, so mask everything beyond the first 4 bits afterwards
            int highBitsInt = (b >> 4) & 0xF;
            // We only want the first 4 bits
            int lowBitsInt = b & 0xF;

            builder.append(HEX_CHARS[highBitsInt]);
            builder.append(HEX_CHARS[lowBitsInt]);
        }

        return builder.toString();
    }

    //----- Internal implementation ------------------------------------------//

    private boolean hasTypeEncodingPrefix(String stringId) {
        return hasAmqpBinaryPrefix(stringId) || hasAmqpUuidPrefix(stringId) ||
               hasAmqpUlongPrefix(stringId) || hasAmqpStringPrefix(stringId);
    }

    private boolean hasAmqpStringPrefix(String stringId) {
        return stringId.startsWith(AMQP_STRING_PREFIX);
    }

    private boolean hasAmqpUlongPrefix(String stringId) {
        return stringId.startsWith(AMQP_ULONG_PREFIX);
    }

    private boolean hasAmqpUuidPrefix(String stringId) {
        return stringId.startsWith(AMQP_UUID_PREFIX);
    }

    private boolean hasAmqpBinaryPrefix(String stringId) {
        return stringId.startsWith(AMQP_BINARY_PREFIX);
    }

    private String strip(String id, int numChars) {
        return id.substring(numChars);
    }

    private int hexCharToInt(char ch, String orig) throws IllegalArgumentException {
        if (ch >= '0' && ch <= '9') {
            // subtract '0' to get difference in position as an int
            return ch - '0';
        } else if (ch >= 'A' && ch <= 'F') {
            // subtract 'A' to get difference in position as an int
            // and then add 10 for the offset of 'A'
            return ch - 'A' + 10;
        } else if (ch >= 'a' && ch <= 'f') {
            // subtract 'a' to get difference in position as an int
            // and then add 10 for the offset of 'a'
            return ch - 'a' + 10;
        }

        throw new IllegalArgumentException("The provided hex string contains non-hex character '" + ch + "': " + orig);
    }
}
