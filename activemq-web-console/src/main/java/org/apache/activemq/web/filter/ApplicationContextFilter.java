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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

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
 * @version $Revision$
 */
public class ApplicationContextFilter implements Filter {

    private ServletContext servletContext;
    private String applicationContextName = "applicationContext";
    private String requestContextName = "requestContext";
    private String requestName = "request";

    public void init(FilterConfig config) throws ServletException {
        this.servletContext = config.getServletContext();
        this.applicationContextName = getInitParameter(config, "applicationContextName", applicationContextName);
        this.requestContextName = getInitParameter(config, "requestContextName", requestContextName);
        this.requestName = getInitParameter(config, "requestName", requestName);

        // register the application context in the applicationScope
        WebApplicationContext context = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        Map wrapper = createApplicationContextWrapper(context);
        servletContext.setAttribute(applicationContextName, wrapper);
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        // lets register a requestContext in the requestScope
        Map requestContextWrapper = createRequestContextWrapper(request);
        request.setAttribute(requestContextName, requestContextWrapper);
        request.setAttribute(requestName, request);
        chain.doFilter(request, response);
    }

    public void destroy() {
    }

    public ServletContext getServletContext() {
        return servletContext;
    }

    public String getApplicationContextName() {
        return applicationContextName;
    }

    public void setApplicationContextName(String variableName) {
        this.applicationContextName = variableName;
    }

    public String getRequestContextName() {
        return requestContextName;
    }

    public void setRequestContextName(String requestContextName) {
        this.requestContextName = requestContextName;
    }

    protected String getInitParameter(FilterConfig config, String key, String defaultValue) {
        String parameter = config.getInitParameter(key);
        return (parameter != null) ? parameter : defaultValue;
    }

    /**
     * Creates a wrapper around the web application context so that it can be
     * accessed easily from inside JSP EL (or other expression languages in
     * other view technologies).
     */
    protected Map createApplicationContextWrapper(final WebApplicationContext context) {
        Map wrapper = new AbstractMap() {

            public WebApplicationContext getContext() {
                return context;
            }

            public Object get(Object key) {
                if (key == null) {
                    return null;
                }
                return context.getBean(key.toString());
            }

            public Set entrySet() {
                return Collections.EMPTY_SET;
            }

        };
        return wrapper;
    }

    /**
     * Creates a wrapper around the request context (e.g. to allow POJOs to be
     * auto-injected from request parameter values etc) so that it can be
     * accessed easily from inside JSP EL (or other expression languages in
     * other view technologies).
     */
    protected Map createRequestContextWrapper(final ServletRequest request) {
        final WebApplicationContext context = WebApplicationContextUtils.getWebApplicationContext(servletContext);
        Map wrapper = new AbstractMap() {

            public WebApplicationContext getContext() {
                return context;
            }

            public Object get(Object key) {
                if (key == null) {
                    return null;
                }
                return bindRequestBean(context.getBean(key.toString()), request);
            }

            public Set entrySet() {
                return Collections.EMPTY_SET;
            }

        };
        return wrapper;

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
