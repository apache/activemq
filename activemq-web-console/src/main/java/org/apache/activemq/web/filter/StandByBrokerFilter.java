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
package org.apache.activemq.web.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.activemq.web.BrokerFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * When the broker is running in "slave" (standby) mode, this filter redirects the Web Console to a page that only communicates
 * this mode. This is because all interactions via Web Console should be done in the "master" (active) broker.
 */
public class StandByBrokerFilter extends HttpFilter {
    private static final Logger LOG = LoggerFactory.getLogger(StandByBrokerFilter.class);
    private static final String STANDBYPAGE = "slave.jsp";

    @Override
    protected void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)  throws IOException, ServletException {
        final Map<?, ?> context = getContextOrTrow(request);
        final String path = request.getRequestURI();

        try {
            final boolean isStandBy = ((BrokerFacade) context.get("brokerQuery")).getBrokerAdmin().isSlave();
            if (isStandBy) {
                if (!(path.endsWith("css") || path.endsWith("png") || path.endsWith("ico") || path.endsWith(STANDBYPAGE))) {
                    LOG.debug("In StandBy mode, redirecting {} to {}", path, STANDBYPAGE);
                    response.sendRedirect(STANDBYPAGE);
                    return;
                }
            } else {
                if (path.endsWith(STANDBYPAGE)) {
                    LOG.debug("Not in StandBy mode, redirecting {} to '/index.jsp'", path);
                    response.sendRedirect(request.getContextPath() + "/index.jsp");
                    return;
                }
            }
        } catch (Exception e) {
            LOG.warn("{}, failed to access BrokerFacade: reason: {}", path, e.getLocalizedMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug(request.toString(), e);
            }
            throw new IOException(e);
        }

        chain.doFilter(request, response);
    }

    private Map<?, ?> getContextOrTrow(final HttpServletRequest request) throws IllegalStateException {
        final Object contextAttributeKey = request.getAttribute(ApplicationContextFilter.CONTEXT_NAME_ATTR);
        if (contextAttributeKey == null) {
            // Make sure ApplicationContextFilter is executed before StandByBrokerFilter
            throw new IllegalStateException("Request context attribute name not found");
        }

        final Object contextAttribute = request.getAttribute((String) contextAttributeKey);
        if (contextAttribute == null) {
            // Make sure ApplicationContextFilter is executed before StandByBrokerFilter
            throw new IllegalStateException("Request context attribute not found");
        }

        if (! (contextAttribute instanceof Map<?, ?>) ) {
            throw new IllegalStateException(String.format("Unexpected Request Context format: %s", contextAttribute.getClass()));
        }

        return (Map<?, ?>) contextAttribute;
    }
}
