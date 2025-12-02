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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static java.util.Map.entry;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.activemq.web.controller.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.ServletRequestDataBinder;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * Servlet that routes requests to the appropriate controller class and injects the request parameters into it.
 */
public class ActionServlet extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(ActionServlet.class);

    private static final Map<String, Class<? extends Controller>> routes = Map.ofEntries(
        entry("/createDestination.action", CreateDestination.class),
        entry("/deleteDestination.action", DeleteDestination.class),
        entry("/createSubscriber.action", CreateSubscriber.class),
        entry("/deleteSubscriber.action", DeleteSubscriber.class),
        entry("/sendMessage.action", SendMessage.class),
        entry("/purgeDestination.action", PurgeDestination.class),
        entry("/deleteMessage.action", DeleteMessage.class),
        entry("/copyMessage.action", CopyMessage.class),
        entry("/moveMessage.action", MoveMessage.class),
        entry("/deleteJob.action", DeleteJob.class),
        entry("/retryMessage.action", RetryMessage.class),
        entry("/pauseDestination.action", PauseDestination.class),
        entry("/resumeDestination.action", ResumeDestination.class)
    );

    private WebApplicationContext context;

    @Override
    public void init() throws ServletException {
        super.init();
        context = WebApplicationContextUtils.getWebApplicationContext(getServletContext());
        if (context == null) {
            throw new IllegalStateException("Failed to initialize Web Application Context");
        }
        LOG.info("Action Servlet initialized");
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            LOG.info("Received request for action with path {}", request.getRequestURI());

            final String actionPath = getAction(request);
            final Optional<Controller> maybeController = getHandler(actionPath);
            if (maybeController.isEmpty()) {
                // Path does not route to any request
                LOG.warn("No action is mapped to path: {} ({})", request.getRequestURI(), actionPath);
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }

            final Controller controller = maybeController.get();

            checkMethodIsSupported(controller, request);
            injectRequestDataIntoController(controller, request);

            LOG.info("Handling action: {} with {}", actionPath, controller);

            controller.handleRequest(request, response);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private String getAction(final HttpServletRequest request) {
        String path = request.getRequestURI();
        if (path.contains("/")) {
            path = path.substring(path.lastIndexOf("/"));
        }
        return path;
    }

    private Optional<Controller> getHandler(final String path) {
        return Optional.ofNullable(routes.get(path)).map(context::getBean);
    }

    private void checkMethodIsSupported(final Controller controller, final HttpServletRequest request) {
        if (controller instanceof DestinationFacade destination) {
            if (!Arrays.asList(destination.getSupportedHttpMethods()).contains(request.getMethod())) {
                throw new UnsupportedOperationException("Unsupported method " + request.getMethod());
            }
        }
    }

    private void injectRequestDataIntoController(final Controller controller, final HttpServletRequest request) {
        final ServletRequestDataBinder binder = new ServletRequestDataBinder(controller);
        binder.bind(request);
    }
}