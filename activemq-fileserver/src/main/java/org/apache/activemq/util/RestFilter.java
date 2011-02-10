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
//========================================================================
//Copyright 2007 CSC - Scientific Computing Ltd.
//========================================================================
package org.apache.activemq.util;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Adds support for HTTP PUT, MOVE and DELETE methods. If init parameters
 * read-permission-role and write-permission-role are defined then all requests
 * are authorized using the defined roles. Also GET methods are authorized.
 * </p>
 * 
 * @author Aleksi Kallio
 */
public class RestFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RestFilter.class);

    private static final String HTTP_HEADER_DESTINATION = "Destination";
    private static final String HTTP_METHOD_MOVE = "MOVE";
    private static final String HTTP_METHOD_PUT = "PUT";
    private static final String HTTP_METHOD_GET = "GET";
    private static final String HTTP_METHOD_DELETE = "DELETE";

    private String readPermissionRole;
    private String writePermissionRole;
    private FilterConfig filterConfig;

    public void init(FilterConfig filterConfig) throws UnavailableException {
        this.filterConfig = filterConfig;
        readPermissionRole = filterConfig.getInitParameter("read-permission-role");
        writePermissionRole = filterConfig.getInitParameter("write-permission-role");
    }

    private File locateFile(HttpServletRequest request) {
        return new File(filterConfig.getServletContext().getRealPath(request.getServletPath()), request.getPathInfo());
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest && response instanceof HttpServletResponse)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request not HTTP, can not understand: " + request.toString());
            }
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest httpRequest = (HttpServletRequest)request;
        HttpServletResponse httpResponse = (HttpServletResponse)response;

        if (httpRequest.getMethod().equals(HTTP_METHOD_MOVE)) {
            doMove(httpRequest, httpResponse);
        } else if (httpRequest.getMethod().equals(HTTP_METHOD_PUT)) {
            doPut(httpRequest, httpResponse);
        } else if (httpRequest.getMethod().equals(HTTP_METHOD_GET)) {
            if (checkGet(httpRequest, httpResponse)) {
                chain.doFilter(httpRequest, httpResponse); // actual processing
                                                            // done elsewhere
            }
        } else if (httpRequest.getMethod().equals(HTTP_METHOD_DELETE)) {
            doDelete(httpRequest, httpResponse);
        } else {
            chain.doFilter(httpRequest, httpResponse);
        }
    }

    protected void doMove(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("RESTful file access: MOVE request for " + request.getRequestURI());
        }

        if (writePermissionRole != null && !request.isUserInRole(writePermissionRole)) {
            response.sendError(HttpURLConnection.HTTP_FORBIDDEN);
            return;
        }

        File file = locateFile(request);
        String destination = request.getHeader(HTTP_HEADER_DESTINATION);

        if (destination == null) {
            response.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Destination header not found");
            return;
        }

        try {
            URL destinationUrl = new URL(destination);
            IOHelper.copyFile(file, new File(destinationUrl.getFile()));
            IOHelper.deleteFile(file);
        } catch (IOException e) {
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR); // file
                                                                        // could
                                                                        // not
                                                                        // be
                                                                        // moved
            return;
        }

        response.setStatus(HttpURLConnection.HTTP_NO_CONTENT); // we return no
                                                                // content
    }

    protected boolean checkGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("RESTful file access: GET request for " + request.getRequestURI());
        }

        if (readPermissionRole != null && !request.isUserInRole(readPermissionRole)) {
            response.sendError(HttpURLConnection.HTTP_FORBIDDEN);
            return false;
        } else {
            return true;
        }
    }

    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("RESTful file access: PUT request for " + request.getRequestURI());
        }

        if (writePermissionRole != null && !request.isUserInRole(writePermissionRole)) {
            response.sendError(HttpURLConnection.HTTP_FORBIDDEN);
            return;
        }

        File file = locateFile(request);

        if (file.exists()) {
            boolean success = file.delete(); // replace file if it exists
            if (!success) {
                response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR); // file
                                                                            // existed
                                                                            // and
                                                                            // could
                                                                            // not
                                                                            // be
                                                                            // deleted
                return;
            }
        }

        FileOutputStream out = new FileOutputStream(file);
        try {
            IOHelper.copyInputStream(request.getInputStream(), out);
        } catch (IOException e) {
            LOG.warn("Exception occured" , e);
            out.close();
            throw e;
        }

        response.setStatus(HttpURLConnection.HTTP_NO_CONTENT); // we return no
                                                                // content
    }

    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("RESTful file access: DELETE request for " + request.getRequestURI());
        }

        if (writePermissionRole != null && !request.isUserInRole(writePermissionRole)) {
            response.sendError(HttpURLConnection.HTTP_FORBIDDEN);
            return;
        }

        File file = locateFile(request);

        if (!file.exists()) {
            response.sendError(HttpURLConnection.HTTP_NOT_FOUND); // file not
                                                                    // found
            return;
        }

        boolean success = IOHelper.deleteFile(file); // actual delete operation

        if (success) {
            response.setStatus(HttpURLConnection.HTTP_NO_CONTENT); // we return
                                                                    // no
                                                                    // content
        } else {
            response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR); // could
                                                                        // not
                                                                        // be
                                                                        // deleted
                                                                        // due
                                                                        // to
                                                                        // internal
                                                                        // error
        }
    }

    public void destroy() {
        // nothing to destroy
    }
}
