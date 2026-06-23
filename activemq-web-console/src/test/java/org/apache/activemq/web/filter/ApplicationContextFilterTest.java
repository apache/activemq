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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import jakarta.servlet.ServletException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.StaticWebApplicationContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ApplicationContextFilterTest {
    private ApplicationContextFilter sut;

    @Before
    public void setup() {
        sut = new ApplicationContextFilter();
    }

    @Test
    public void doFilterPopulatesRequestAttributes() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();

        sut.init(new MockFilterConfig());
        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

        assertNotNull("Request Context Name not found", request.getAttribute("requestContextName"));
        assertNotNull("Request Context not found", request.getAttribute("requestContext"));
        assertTrue("Request Context does not have the expected type", request.getAttribute("requestContext") instanceof Map<?,?>);
        assertNotNull("Request attribute not found", request.getAttribute("request"));
    }

    @Test
    public void filterChainIsCalled() throws ServletException, IOException {
        final MockFilterChain chain = new MockFilterChain();

        sut.init(new MockFilterConfig());
        sut.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), chain);

        assertNotNull("Request was not chained to the next filter", chain.getRequest());
        assertNotNull("Response was not chained to the next filter", chain.getResponse());
    }

    @Test
    public void useContextAttributeNameFromConfig() throws ServletException, IOException {
        final String contextAttrName = "customContext";

        final MockFilterConfig config = new MockFilterConfig();
        config.addInitParameter("requestContextName", contextAttrName);

        final MockHttpServletRequest request = new MockHttpServletRequest();

        sut.init(config);
        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

        assertEquals("Request attribute with context key name incorrect", contextAttrName, request.getAttribute("requestContextName"));
        assertNotNull("Request attribute with custom name not found", request.getAttribute(contextAttrName));
        assertTrue("Request Context does not have the expected type", request.getAttribute(contextAttrName) instanceof Map<?,?>);
    }

    @Test
    public void contextHasExpectedValues() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        final MockServletContext servletContext = new MockServletContext();
        final StaticWebApplicationContext appContext = new StaticWebApplicationContext();

        servletContext.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, appContext);

        final String sampleContextKey = "foo";
        final String sampleContextValue = "bar";
        appContext.getBeanFactory().registerSingleton(sampleContextKey, sampleContextValue);

        sut.init(new MockFilterConfig(servletContext));
        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());

        @SuppressWarnings("unchecked")
        final Map<Object, Object> context = (Map<Object, Object>) request.getAttribute("requestContext");

        assertNotNull(context);
        assertEquals("Context Entry Set is not empty", Collections.EMPTY_SET, context.entrySet());
        assertEquals("Context does not have the expected value", sampleContextValue, context.get(sampleContextKey));
        assertNull("Context failed to handle null keys", context.get(null));
    }
}
