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

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class FilenameGuardFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(FilenameGuardFilter.class);
    
    public void destroy() {
        // nothing to destroy
    }

    public void init(FilterConfig config) throws ServletException {
        // nothing to init
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest httpRequest = (HttpServletRequest)request;
            GuardedHttpServletRequest guardedRequest = new GuardedHttpServletRequest(httpRequest);
            chain.doFilter(guardedRequest, response);
        } else {
            chain.doFilter(request, response);
        }
    }

    private static class GuardedHttpServletRequest extends HttpServletRequestWrapper {

        public GuardedHttpServletRequest(HttpServletRequest httpRequest) {
            super(httpRequest);
        }

        private String guard(String filename) {
            String guarded = filename.replace(":", "_");
            if (LOG.isDebugEnabled()) {
                LOG.debug("guarded " + filename + " to " + guarded);
            }
            return guarded;
        }

        @Override
        public String getParameter(String name) {
            if (name.equals("Destination")) {
                return guard(super.getParameter(name));
            } else {
                return super.getParameter(name);
            }
        }

        @Override
        public String getPathInfo() {
            return guard(super.getPathInfo());
        }

        @Override
        public String getPathTranslated() {
            return guard(super.getPathTranslated());
        }

        @Override
        public String getRequestURI() {
            return guard(super.getRequestURI());
        }
    }
}
