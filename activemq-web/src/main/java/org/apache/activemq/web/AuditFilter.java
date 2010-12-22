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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;

public class AuditFilter implements Filter {

    private static final Log LOG = LogFactory.getLog("org.apache.activemq.audit");

    private boolean audit;

    public void init(FilterConfig filterConfig) throws ServletException {
       audit = "true".equalsIgnoreCase(System.getProperty("org.apache.activemq.audit"));
    }

    public void destroy() {
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (audit && request instanceof HttpServletRequest) {

            HttpServletRequest http = (HttpServletRequest)request;
            Enumeration params = http.getParameterNames();
            String formattedParams = "";
            while (params.hasMoreElements()) {
                String paramName = (String)params.nextElement();
                String paramValue = http.getParameter(paramName);
                formattedParams += paramName + "='" + paramValue + "' ";
            }
            String user = "anonymous";
            if (http.getRemoteUser() != null) {
                user = http.getRemoteUser();
            }
            LOG.info(user + " requested " + http.getRequestURI() + " [" + formattedParams + "] from " + http.getRemoteAddr());
        }
        chain.doFilter(request, response);
    }
}
