/*
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
package org.apache.activemq.transport.amqp.message;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.StringTokenizer;

public final class AmqpContentTypeSupport {

    private static final String UTF_8 = "UTF-8";
    private static final String CHARSET = "charset";
    private static final String TEXT = "text";
    private static final String APPLICATION = "application";
    private static final String JAVASCRIPT = "javascript";
    private static final String XML = "xml";
    private static final String XML_VARIANT = "+xml";
    private static final String JSON = "json";
    private static final String JSON_VARIANT = "+json";
    private static final String XML_DTD = "xml-dtd";
    private static final String ECMASCRIPT = "ecmascript";

    /**
     * @param contentType
     *        the contentType of the received message
     * @return the character set to use, or null if not to treat the message as
     *         text
     * @throws InvalidContentTypeException
     *         if the content-type is invalid in some way.
     */
    public static Charset parseContentTypeForTextualCharset(final String contentType) throws InvalidContentTypeException {
        if (contentType == null || contentType.trim().isEmpty()) {
            throw new InvalidContentTypeException("Content type can't be null or empty");
        }

        int subTypeSeparator = contentType.indexOf("/");
        if (subTypeSeparator == -1) {
            throw new InvalidContentTypeException("Content type has no '/' separator: " + contentType);
        }

        final String type = contentType.substring(0, subTypeSeparator).toLowerCase().trim();

        String subTypePart = contentType.substring(subTypeSeparator + 1).toLowerCase().trim();

        String parameterPart = null;
        int parameterSeparator = subTypePart.indexOf(";");
        if (parameterSeparator != -1) {
            if (parameterSeparator < subTypePart.length() - 1) {
                parameterPart = contentType.substring(subTypeSeparator + 1).toLowerCase().trim();
            }
            subTypePart = subTypePart.substring(0, parameterSeparator).trim();
        }

        if (subTypePart.isEmpty()) {
            throw new InvalidContentTypeException("Content type has no subtype after '/'" + contentType);
        }

        final String subType = subTypePart;

        if (isTextual(type, subType)) {
            String charset = findCharset(parameterPart);
            if (charset == null) {
                charset = UTF_8;
            }

            if (UTF_8.equals(charset)) {
                return StandardCharsets.UTF_8;
            } else {
                try {
                    return Charset.forName(charset);
                } catch (IllegalCharsetNameException icne) {
                    throw new InvalidContentTypeException("Illegal charset: " + charset);
                } catch (UnsupportedCharsetException uce) {
                    throw new InvalidContentTypeException("Unsupported charset: " + charset);
                }
            }
        }

        return null;
    }

    //----- Internal Content Type utilities ----------------------------------//

    private static boolean isTextual(String type, String subType) {
        if (TEXT.equals(type)) {
            return true;
        }

        if (APPLICATION.equals(type)) {
            if (XML.equals(subType) || JSON.equals(subType) || JAVASCRIPT.equals(subType) || subType.endsWith(XML_VARIANT) || subType.endsWith(JSON_VARIANT)
                || XML_DTD.equals(subType) || ECMASCRIPT.equals(subType)) {
                return true;
            }
        }

        return false;
    }

    private static String findCharset(String paramaterPart) {
        String charset = null;

        if (paramaterPart != null) {
            StringTokenizer tokenizer = new StringTokenizer(paramaterPart, ";");
            while (tokenizer.hasMoreTokens()) {
                String parameter = tokenizer.nextToken().trim();
                int eqIndex = parameter.indexOf('=');
                if (eqIndex != -1) {
                    String name = parameter.substring(0, eqIndex);
                    if (CHARSET.equalsIgnoreCase(name.trim())) {
                        String value = unquote(parameter.substring(eqIndex + 1));

                        charset = value.toUpperCase();
                        break;
                    }
                }
            }
        }

        return charset;
    }

    private static String unquote(String s) {
        if (s.length() > 1 && (s.startsWith("\"") && s.endsWith("\""))) {
            return s.substring(1, s.length() - 1);
        } else {
            return s;
        }
    }
}
