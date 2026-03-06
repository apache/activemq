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
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.ServletRequestDataBinder;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * Exposes Spring ApplicationContexts to JSP EL and other view technologies.
 * Currently a variable is placed in application scope (by default called
 * 'applicationContext') so that POJOs can be pulled out of Spring in a JSP page
 * to render things using EL expressions. <br/>
 * 
 * e.g. ${applicationContext.cheese} would access the cheese POJO. Or
 * ${applicationContext.cheese.name} would access the name property of the
 * cheese POJO. <br/>
 * 
 * You can then use JSTL to work with these POJOs such as &lt;c.set var="myfoo"
 * value="${applicationContext.foo}"/&gt; <br/>
 * 
 * In addition to applicationContext a 'requestContext' variable is created
 * which will automatically bind any request parameters to the POJOs extracted
 * from the applicationContext - which is ideal for POJOs which implement
 * queries in view technologies.
 * 
 * 
 */
public class ApplicationContextFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationContextFilter.class);

    private ServletContext servletContext;
    private String requestContextName = "requestContext";

    public void init(FilterConfig config) throws ServletException {
        this.servletContext = config.getServletContext();
        this.requestContextName = getInitParameter(config, "requestContextName", requestContextName);

        LOG.info("Application Context Filter initialized. Request context is available in '{}'", requestContextName);
    }

    private String getInitParameter(final FilterConfig config, final String key, final String defaultValue) {
        String parameter = config.getInitParameter(key);
        return (parameter != null) ? parameter : defaultValue;
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        // lets register a requestContext in the requestScope
        request.setAttribute(requestContextName, createRequestContextWrapper(request));
        chain.doFilter(request, response);
    }

    /**
     * Creates a wrapper around the request context (e.g. to allow POJOs to be
     * auto-injected from request parameter values etc) so that it can be
     * accessed easily from inside JSP EL (or other expression languages in
     * other view technologies).
     */
    private Map createRequestContextWrapper(final ServletRequest request) {
        final WebApplicationContext context = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        return new AbstractMap<>() {
            public Object get(Object key) {
                if (key == null) {
                    return null;
                }
                final Object emptyBean = context.getBean(key.toString());
                return bindRequestBean(emptyBean, request);
            }

            public Set entrySet() {
                return Collections.EMPTY_SET;
            }
        };
    }

    /**
     * Binds properties from the request parameters to the given POJO which is
     * useful for POJOs which are configurable via request parameters such as
     * for query/view POJOs
     */
    protected Object bindRequestBean(Object bean, ServletRequest request) {
        ServletRequestDataBinder binder = new ServletRequestDataBinder(bean, null);
        binder.bind(request);
        return bean;
    }
}
