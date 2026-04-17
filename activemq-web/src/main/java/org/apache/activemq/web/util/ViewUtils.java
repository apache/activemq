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
package org.apache.activemq.web.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ViewUtils {

    public static final String AMP = "&";
    public static final String QUOTE = "\"";
    public static final String LT = "<";
    public static final String GT = ">";
    public static final String APOS = "'";

    public static final String XML_ESCAPED_AMP = "&amp;";
    public static final String XML_ESCAPED_QUOTE = "&quot;";
    public static final String XML_ESCAPED_LT = "&lt;";
    public static final String XML_ESCAPED_GT = "&gt;";
    public static final String XML_ESCAPED_APOS = "&apos;";

    public static final Map<String, String> XML_ESCAPE_MAPPINGS;

    static {
        // order matters for processing so use a linked map
        Map<String,String> mappings = new LinkedHashMap<>();
        mappings.put(AMP, XML_ESCAPED_AMP);
        mappings.put(LT, XML_ESCAPED_LT);
        mappings.put(GT, XMl_ESCAPED_GT);
        mappings.put(QUOTE, XML_ESCAPED_QUOTE);
        mappings.put(APOS, XML_ESCAPED_APOS);

        XML_ESCAPE_MAPPINGS = Collections.unmodifiableMap(mappings);
    }


    public static String escapeXml(String input) {
        if (input == null) {
            return null;
        }

        String escaped = input;
        for (Entry<String, String> entry : XML_ESCAPE_MAPPINGS.entrySet()) {
            escaped = escaped.replace(entry.getKey(), entry.getValue());
        }

        return escaped;
    }

}
