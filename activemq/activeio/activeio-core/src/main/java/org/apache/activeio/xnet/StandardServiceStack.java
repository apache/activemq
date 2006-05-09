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
import org.apache.activeio.xnet.hba.ServiceAccessController;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class StandardServiceStack {

    private String name;

    private ServiceDaemon daemon;
    private ServiceLogger logger;
    private ServiceAccessController hba;
    private ServicePool pool;
    private ServerService server;
    private String host;

    public StandardServiceStack(String name, int port, String host, IPAddressPermission[] allowHosts, String[] logOnSuccess, String[] logOnFailure, Executor executor, ServerService server) throws UnknownHostException {
        this.server = server;
        this.name = name;
        this.host = host;
        InetAddress address = InetAddress.getByName(host);
        this.pool = new ServicePool(server, executor);
        this.hba = new ServiceAccessController(name, pool, allowHosts);
        this.logger = new ServiceLogger(name, hba, logOnSuccess, logOnFailure);
        this.daemon = new ServiceDaemon(name, logger, address, port);

    }

    public String getName() {
        return name;
    }

    public InetAddress getAddress() {
        return daemon.getAddress();
    }

    public InetSocketAddress getFullAddress() {
        return new InetSocketAddress(getAddress(), getPort());
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return daemon.getPort();
    }

    public int getSoTimeout() throws IOException {
        return daemon.getSoTimeout();
    }

    public void setSoTimeout(int timeout) throws SocketException {
        daemon.setSoTimeout(timeout);
    }

    public String[] getLogOnSuccess() {
        return logger.getLogOnSuccess();
    }

    public String[] getLogOnFailure() {
        return logger.getLogOnFailure();
    }

    public IPAddressPermission[] getAllowHosts() {
        return hba.getAllowHosts();
    }

    public void setAllowHosts(IPAddressPermission[] allowHosts) {
        hba.setAllowHosts(allowHosts);
    }

    public void doStart() throws Exception {
        daemon.start();
    }

    public void doStop() throws Exception {
        daemon.stop();
    }

    public void doFail() {
        try {
            daemon.stop();
        } catch (ServiceException dontCare) {            
        }
    }
}
