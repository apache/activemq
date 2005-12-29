/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.xnet;

import edu.emory.mathcs.backport.java.util.concurrent.Executor;

import org.apache.activeio.xnet.hba.IPAddressPermission;
import org.apache.geronimo.gbean.GBeanData;
import org.apache.geronimo.gbean.GBeanInfo;
import org.apache.geronimo.gbean.GBeanInfoBuilder;
import org.apache.geronimo.gbean.GBeanLifecycle;
import org.apache.geronimo.j2ee.j2eeobjectnames.NameFactory;
import org.apache.geronimo.kernel.GBeanAlreadyExistsException;
import org.apache.geronimo.kernel.GBeanNotFoundException;
import org.apache.geronimo.kernel.Kernel;
import org.apache.geronimo.kernel.jmx.JMXUtil;

import javax.management.ObjectName;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.net.SocketException;
import java.io.IOException;

public class StandardServiceStackGBean implements GBeanLifecycle {
    private final StandardServiceStack standardServiceStack;

    public StandardServiceStackGBean(String name, int port, String host, IPAddressPermission[] allowHosts, String[] logOnSuccess, String[] logOnFailure, Executor executor, ServerService server) throws UnknownHostException {
        standardServiceStack = new StandardServiceStack(name, port, host, allowHosts, logOnSuccess, logOnFailure, executor, server);
    }

    public String getName() {
        return standardServiceStack.getName();
    }

    public InetAddress getAddress() {
        return standardServiceStack.getAddress();
    }

    public InetSocketAddress getFullAddress() {
        return standardServiceStack.getFullAddress();
    }

    public String getHost() {
        return standardServiceStack.getHost();
    }

    public int getPort() {
        return standardServiceStack.getPort();
    }

    public int getSoTimeout() throws IOException {
        return standardServiceStack.getSoTimeout();
    }

    public void setSoTimeout(int timeout) throws SocketException {
        standardServiceStack.setSoTimeout(timeout);
    }

    public String[] getLogOnSuccess() {
        return standardServiceStack.getLogOnSuccess();
    }

    public String[] getLogOnFailure() {
        return standardServiceStack.getLogOnFailure();
    }

    public IPAddressPermission[] getAllowHosts() {
        return standardServiceStack.getAllowHosts();
    }

    public void setAllowHosts(IPAddressPermission[] allowHosts) {
        standardServiceStack.setAllowHosts(allowHosts);
    }

    public void doStart() throws Exception {
        standardServiceStack.doStart();
    }

    public void doStop() throws Exception {
        standardServiceStack.doStop();
    }

    public void doFail() {
        standardServiceStack.doFail();
    }

    public static final GBeanInfo GBEAN_INFO;

    static {
        GBeanInfoBuilder infoFactory = new GBeanInfoBuilder("ActiveIO Connector", StandardServiceStackGBean.class);

        infoFactory.addAttribute("name", String.class, true);
        infoFactory.addAttribute("port", int.class, true, true);
        infoFactory.addAttribute("soTimeout", int.class, true);
        infoFactory.addAttribute("host", String.class, true, true);
        infoFactory.addAttribute("fullAddress", InetSocketAddress.class, false);
        infoFactory.addAttribute("allowHosts", IPAddressPermission[].class, true);
        infoFactory.addAttribute("logOnSuccess", String[].class, true);
        infoFactory.addAttribute("logOnFailure", String[].class, true);

        infoFactory.addReference("Executor", Executor.class, NameFactory.GERONIMO_SERVICE);
        infoFactory.addReference("Server", ServerService.class, NameFactory.GERONIMO_SERVICE);

        infoFactory.setConstructor(new String[]{
            "name",
            "port",
            "host",
            "allowHosts",
            "logOnSuccess",
            "logOnFailure",
            "Executor",
            "Server"});

        GBEAN_INFO = infoFactory.getBeanInfo();
    }

    public static GBeanInfo getGBeanInfo() {
        return GBEAN_INFO;
    }

    public static ObjectName addGBean(Kernel kernel, String name, int port, String host, InetAddress[] allowHosts, String[] logOnSuccess, String[] logOnFailure, ObjectName executor, ObjectName server) throws GBeanAlreadyExistsException, GBeanNotFoundException {
        ClassLoader classLoader = StandardServiceStack.class.getClassLoader();
        ObjectName SERVICE_NAME = JMXUtil.getObjectName("activeio:type=StandardServiceStack,name=" + name);

        GBeanData gbean = new GBeanData(SERVICE_NAME, StandardServiceStackGBean.GBEAN_INFO);

        gbean.setAttribute("name", name);
        gbean.setAttribute("port", new Integer(port));
        gbean.setAttribute("host", host);
        gbean.setAttribute("allowHosts", allowHosts);
        gbean.setAttribute("logOnSuccess", logOnSuccess);
        gbean.setAttribute("logOnFailure", logOnFailure);

        gbean.setReferencePattern("Executor", executor);
        gbean.setReferencePattern("Server", server);

        kernel.loadGBean(gbean, classLoader);
        kernel.startGBean(SERVICE_NAME);
        return SERVICE_NAME;
    }
}
