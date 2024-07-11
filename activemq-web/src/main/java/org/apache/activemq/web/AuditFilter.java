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
package org.apache.activemq.web;

import org.apache.activemq.broker.util.AuditLogEntry;
import org.apache.activemq.broker.util.AuditLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AuditFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger("org.apache.activemq.audit");
    private static final String[] SENSITIVE_PARAM_KEYS = {"JMSText"};
    private boolean audit;
    private AuditLogService auditLog;

    public void init(FilterConfig filterConfig) throws ServletException {
        audit = "true".equalsIgnoreCase(System.getProperty("org.apache.activemq.audit"));
        if (audit) {
            auditLog = AuditLogService.getAuditLog();
        }
    }

    public void destroy() {
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (audit && request instanceof HttpServletRequest) {
            HttpServletRequest http = (HttpServletRequest)request;
            AuditLogEntry entry = new HttpAuditLogEntry();
            if (http.getRemoteUser() != null) {
                entry.setUser(http.getRemoteUser());
            }
            entry.setTimestamp(System.currentTimeMillis());
            entry.setOperation(http.getRequestURI());
            entry.setRemoteAddr(http.getRemoteAddr());
            entry.getParameters().put("params", redactSensitiveHttpParameters(http.getParameterMap()));
            auditLog.log(entry);
        }
        chain.doFilter(request, response);
    }

    /**
     * Redact sensitive data present in HTTP params
     */
    public Map<String, String[]> redactSensitiveHttpParameters(Map<String, String[]> httpParams) {
        final Map<String, String[]> sanitizedParamsMap = new HashMap<>(httpParams);
        final String[] redactedEntry = {"*****"};
        for (String param : SENSITIVE_PARAM_KEYS) {
            if (sanitizedParamsMap.containsKey(param)) {
                sanitizedParamsMap.put(param, redactedEntry);
            }
        }
        return sanitizedParamsMap;
    }
}
