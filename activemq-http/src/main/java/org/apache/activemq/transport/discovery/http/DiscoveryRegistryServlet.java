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
package org.apache.activemq.transport.discovery.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoveryRegistryServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HTTPDiscoveryAgent.class);
    long maxKeepAge = 1000*60*60; // 1 hour.
    ConcurrentMap<String, ConcurrentMap<String, Long>> serviceGroups = new ConcurrentHashMap<String, ConcurrentMap<String, Long>>();

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String group = req.getPathInfo();
        String service = req.getHeader("service");
        LOG.debug("Registering: group="+group+", service="+service);

        ConcurrentMap<String, Long> services = getServiceGroup(group);
        services.put(service, System.currentTimeMillis());
    }

    private ConcurrentMap<String, Long> getServiceGroup(String group) {
        return serviceGroups.computeIfAbsent(group, k -> new ConcurrentHashMap<String, Long>());
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            long freshness = 1000*30;
            String p = req.getParameter("freshness");
            if( p!=null ) {
                freshness = Long.parseLong(p);
            }

            String group = req.getPathInfo();
            LOG.debug("group="+group);
            ConcurrentMap<String, Long> services = getServiceGroup(group);
            PrintWriter writer = resp.getWriter();

            long now = System.currentTimeMillis();
            long dropTime = now-maxKeepAge;
            long minimumTime = now-freshness;

            ArrayList<String> dropList = new ArrayList<String>();
            for (Map.Entry<String, Long> entry : services.entrySet()) {
                if( entry.getValue() > minimumTime ) {
                    writer.println(entry.getKey());
                } else if( entry.getValue() < dropTime ) {
                    dropList.add(entry.getKey());
                }
            }

            // We might as well get rid of the really old entries.
            for (String service : dropList) {
                services.remove(service);
            }


        } catch (Exception e) {
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error occured: "+e);
        }
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String group = req.getPathInfo();
        String service = req.getHeader("service");
        LOG.debug("Unregistering: group="+group+", service="+service);

        ConcurrentMap<String, Long> services = getServiceGroup(group);
        services.remove(service);
    }

}
