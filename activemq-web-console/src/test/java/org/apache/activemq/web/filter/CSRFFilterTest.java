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

import jakarta.servlet.ServletException;

import org.apache.activemq.web.SessionFilter;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.Assert.assertNotNull;

public class CSRFFilterTest {
    private CSRFFilter sut;

    @Before
    public void setup() {
        sut = new CSRFFilter();
    }

    @Test
    public void validSecretAllowsRequestThrough() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        final MockFilterChain chain = new MockFilterChain();
        final String secret = "my-secret-token";

        request.getSession().setAttribute(SessionFilter.SESSION_SECRET_ATTRIBUTE, secret);
        request.setParameter("secret", secret);

        sut.doFilter(request, new MockHttpServletResponse(), chain);

        assertNotNull("Request was not chained to the next filter", chain.getRequest());
        assertNotNull("Response was not chained to the next filter", chain.getResponse());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void missingSessionSecretThrowsException() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.setParameter("secret", "some-value");

        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void missingRequestParameterThrowsException() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.getSession().setAttribute(SessionFilter.SESSION_SECRET_ATTRIBUTE, "my-secret");

        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void mismatchedSecretThrowsException() throws ServletException, IOException {
        final MockHttpServletRequest request = new MockHttpServletRequest();
        request.getSession().setAttribute(SessionFilter.SESSION_SECRET_ATTRIBUTE, "correct-secret");
        request.setParameter("secret", "wrong-secret");

        sut.doFilter(request, new MockHttpServletResponse(), new MockFilterChain());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void noSessionSecretAndNoParameterThrowsException() throws ServletException, IOException {
        sut.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());
    }
}
