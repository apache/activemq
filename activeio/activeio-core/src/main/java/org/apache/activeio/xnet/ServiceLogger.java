/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

public class ServiceLogger implements ServerService {
    private final Log log;
    private final ServerService next;
    private final String[] logOnSuccess;
    private final String[] logOnFailure;
    private final String name;


    public ServiceLogger(String name, ServerService next, String[] logOnSuccess, String[] logOnFailure) {
        this.log = LogFactory.getLog("OpenEJB.server.service." + name);
        this.next = next;
        this.logOnSuccess = logOnSuccess;
        this.logOnFailure = logOnFailure;
        this.name = name;
    }

    /**
     * log_on_success
     * -----------------
     * Different information can be logged when a server starts:
     * <p/>
     * PID : the server's PID (if it's an internal xinetd service, the PID has then a value of 0) ;
     * HOST : the client address ;
     * USERID : the identity of the remote user, according to RFC1413 defining identification protocol;
     * EXIT : the process exit status;
     * DURATION : the session duration.
     * <p/>
     * log_on_failure
     * ------------------
     * Here again, xinetd can log a lot of information when a server can't start, either by lack of resources or because of access rules:
     * HOST, USERID : like above mentioned ;
     * ATTEMPT : logs an access attempt. This an automatic option as soon as another value is provided;
     * RECORD : logs every information available on the client.
     *
     * @param socket
     * @throws org.apache.activeio.xnet.ServiceException
     *
     * @throws IOException
     */
    public void service(Socket socket) throws ServiceException, IOException {
        // Fill this in more deeply later.
        InetAddress client = socket.getInetAddress();
//        MDC.put("HOST", client.getHostName());
//        MDC.put("SERVER", getName());

        try {
            logIncoming();
            next.service(socket);
            logSuccess();
        } catch (Exception e) {
            logFailure(e);
            e.printStackTrace();
        }
    }

    public String[] getLogOnSuccess() {
        return logOnSuccess;
    }

    public String[] getLogOnFailure() {
        return logOnFailure;
    }

    private void logIncoming() {
        log.trace("incomming request");
    }

    private void logSuccess() {
        log.trace("successful request");
    }

    private void logFailure(Exception e) {
        log.error(e.getMessage());
    }


    public void init(Properties props) throws Exception {
        next.init(props);
    }

    public void start() throws ServiceException {
        next.start();
    }

    public void stop() throws ServiceException {
        next.stop();
    }

    public String getName() {
        return next.getName();
    }

    public String getIP() {
        return next.getIP();
    }

    public int getPort() {
        return next.getPort();
    }

}
