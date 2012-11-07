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

import java.util.regex.Pattern;

public final class JMXSupport {

    private static final Pattern PART_1 = Pattern.compile("[\\:\\,\\'\\\"]");
    private static final Pattern PART_2 = Pattern.compile("\\?");
    private static final Pattern PART_3 = Pattern.compile("=");
    private static final Pattern PART_4 = Pattern.compile("\\*");

    private JMXSupport() {
    }

    public static String encodeObjectNamePart(String part) {
        String answer = PART_1.matcher(part).replaceAll("_");
        answer = PART_2.matcher(answer).replaceAll("&qe;");
        answer = PART_3.matcher(answer).replaceAll("&amp;");
        answer = PART_4.matcher(answer).replaceAll("&ast;");
        return answer;
    }

}
