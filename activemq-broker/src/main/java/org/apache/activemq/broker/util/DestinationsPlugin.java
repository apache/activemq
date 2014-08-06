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
package org.apache.activemq.broker.util;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;

/**
 *
 * A simple plugin that can be used to export/import runtime destinations. It's useful in security constrained
 * environments where you want to create destinations only through the management APIs and be able to
 * replicate them to another broker
 *
 * @org.apache.xbean.XBean element="destinationsPlugin"
 */
public class DestinationsPlugin extends BrokerPluginSupport {
    private static Logger LOG = LoggerFactory.getLogger(DestinationsPlugin.class);
    HashSet<ActiveMQDestination> destinations = new HashSet<ActiveMQDestination>();
    File location;

    @Override
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemporary) throws Exception {
        destinations.add(destination);
        return super.addDestination(context, destination, createIfTemporary);
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        super.removeDestination(context, destination, timeout);
        destinations.remove(destination);
    }

    @Override
    public void start() throws Exception {
        super.start();
        if (location == null) {
            location = new File(getBrokerService().getBrokerDataDirectory(), "destinations");
        }
        importDestinations();
        destinations.addAll(Arrays.asList(getBrokerService().getBroker().getDestinations()));
    }

    @Override
    public void stop() throws Exception {
        super.stop();
        exportDestinations();
    }

    protected void importDestinations() throws Exception {
        BufferedReader reader = null;
        try {
            if (location.exists()) {
                reader = new BufferedReader(new FileReader(location));
                String destination;
                Broker broker = getBrokerService().getBroker();
                while ((destination = reader.readLine()) != null) {
                    broker.addDestination(getBrokerService().getAdminConnectionContext(),
                            ActiveMQDestination.createDestination(destination, ActiveMQDestination.QUEUE_TYPE),
                            true);
                }
            }
        } catch (Exception e) {
            LOG.warn("Exception loading destinations", e);
        }  finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    protected void exportDestinations() throws Exception {
        PrintWriter pw = null;
        try {
            location.getParentFile().mkdirs();
            FileOutputStream fos = new FileOutputStream(location);
            pw = new PrintWriter(fos);
            for (ActiveMQDestination destination : destinations) {
                pw.println(destination);
            }
        } catch (Exception e) {
            LOG.warn("Exception saving destinations", e);
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
    }

    public File getLocation() {
        return location;
    }

    public void setLocation(File location) {
        this.location = location;
    }
}
