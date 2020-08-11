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
package org.apache.activemq.transport.util;

import javax.servlet.http.HttpServletRequest;

public class HttpTransportUtils {

    public static String generateWsRemoteAddress(HttpServletRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("HttpServletRequest must not be null.");
        }

        StringBuilder remoteAddress = new StringBuilder();
        String scheme = request.getScheme();
        remoteAddress.append(scheme != null && scheme.equalsIgnoreCase("https") ? "wss://" : "ws://");
        remoteAddress.append(request.getRemoteAddr());
        remoteAddress.append(":");
        remoteAddress.append(request.getRemotePort());
        return remoteAddress.toString();
    }

    public static String generateWsRemoteAddress(HttpServletRequest request, String schemePrefix) {
        if (request == null) {
            throw new IllegalArgumentException("HttpServletRequest must not be null.");
        }

        StringBuilder remoteAddress = new StringBuilder();
        String scheme = request.getScheme();
        if (scheme != null && scheme.equalsIgnoreCase("https")) {
            scheme = schemePrefix + "+wss://";
        } else {
            scheme = schemePrefix + "+ws://";
        }

        remoteAddress.append(scheme);
        remoteAddress.append(request.getRemoteAddr());
        remoteAddress.append(":");
        remoteAddress.append(request.getRemotePort());

        return remoteAddress.toString();
    }
}
