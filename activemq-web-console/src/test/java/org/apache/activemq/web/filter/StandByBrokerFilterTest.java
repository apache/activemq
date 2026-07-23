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
import java.util.Map;

import jakarta.servlet.ServletException;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.web.stubs.StubBrokerFacade;
import org.apache.activemq.web.stubs.StubBrokerViewMBean;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class StandByBrokerFilterTest {
    private static final class ActiveBrokerViewMBean extends StubBrokerViewMBean {
        @Override
        public boolean isSlave() {
            return false;
        }
    }

    private static final class StandByBrokerViewMBean extends StubBrokerViewMBean {
        @Override
        public boolean isSlave() {
            return true;
        }
    }

    private static final class FaultyBrokerViewMBean extends StubBrokerViewMBean {
        @Override
        public boolean isSlave() {
            throw new RuntimeException("Ops");
        }
    }

    private StandByBrokerFilter sut;

    @Before
    public void setup() {
        sut = new StandByBrokerFilter();
    }

    @Test
    public void activeBrokerRedirectsStandByPageToIndex() throws IOException, ServletException{
        final MockHttpServletRequest request = new MockHttpServletRequest();
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        request.setRequestURI("/slave.jsp");
        request.setAttribute("requestContextName", "requestContext");
        request.setAttribute("requestContext", createContext(new ActiveBrokerViewMBean()));

        sut.doFilter(request, response, chain);

        assertEquals("/index.jsp", response.getRedirectedUrl());
        assertNull("Request was changed to the next filter", chain.getRequest());
        assertNull("Response was changed to the next filter", chain.getResponse());
    }

    @Test
    public void standbyBrokerRedirectsToStandByPage() throws IOException, ServletException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        final MockHttpServletResponse response = new MockHttpServletResponse();
        final MockFilterChain chain = new MockFilterChain();

        request.setRequestURI("/queues.jsp");
        request.setAttribute("requestContextName", "requestContext");
        request.setAttribute("requestContext", createContext(new StandByBrokerViewMBean()));

        sut.doFilter(request, response, chain);

        assertEquals("slave.jsp", response.getRedirectedUrl());
        assertNull("Request was changed to the next filter", chain.getRequest());
        assertNull("Response was changed to the next filter", chain.getResponse());
    }

    @Test
    public void activeBrokerChainsRequest() throws IOException, ServletException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        final MockFilterChain chain = new MockFilterChain();
        request.setRequestURI("/index.jsp");
        request.setAttribute("requestContextName", "requestContext");
        request.setAttribute("requestContext", createContext(new ActiveBrokerViewMBean()));

        sut.doFilter(request, new MockHttpServletResponse(), chain);

        assertNotNull("Request was not chained to the next filter", chain.getRequest());
        assertNotNull("Response was not chained to the next filter", chain.getResponse());
    }

    @Test
    public void standbyBrokerAllowsAccessToStandByPage() throws IOException, ServletException {
        standbyBrokerAllowsAccessTo("/slave.jsp");
    }

    @Test
    public void standbyBrokerAllowsAccessToCSS() throws IOException, ServletException {
        standbyBrokerAllowsAccessTo("/style.css");
    }

    @Test
    public void standbyBrokerAllowsAccessToPNG() throws IOException, ServletException {
        standbyBrokerAllowsAccessTo("/logo.png");
    }

    @Test
    public void standbyBrokerAllowsAccessToIco() throws IOException, ServletException {
        standbyBrokerAllowsAccessTo("/favicon.ico");
    }

    private void standbyBrokerAllowsAccessTo(final String path) throws IOException, ServletException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        final MockFilterChain chain = new MockFilterChain();
        request.setRequestURI(path);
        request.setAttribute("requestContextName", "requestContext");
        request.setAttribute("requestContext", createContext(new StandByBrokerViewMBean()));

        sut.doFilter(request, new MockHttpServletResponse(), chain);

        assertNotNull("Request was not chained to the next filter", chain.getRequest());
        assertNotNull("Response was not chained to the next filter", chain.getResponse());
    }

    @Test(expected = IllegalStateException.class)
    public void missingRequestContextAttrNameThrowsException() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.setAttribute("requestContext", createContext(new ActiveBrokerViewMBean()));

        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    @Test(expected = IllegalStateException.class)
    public void missingRequestContextAttrThrowsException() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.setAttribute("requestContextName", "requestContext");

        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    @Test(expected = IllegalStateException.class)
    public void nonMapRequestContextThrowsException() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.setAttribute("requestContextName", "requestContext");
        request.setAttribute("requestContext", "not-a-map");

        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    @Test(expected = IOException.class)
    public void brokerFacadeExceptionWrappedInIOException() throws Exception {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/index.jsp");
        request.setAttribute("requestContextName", "requestContext");
        request.setAttribute("requestContext", createContext(new FaultyBrokerViewMBean()));

        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    private Map<Object, Object> createContext(final BrokerViewMBean brokerViewMBean) {
        return Map.of("brokerQuery", new StubBrokerFacade(brokerViewMBean));
    }
}
