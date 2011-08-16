/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.activemq.console;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.console.command.Command;
import org.apache.activemq.console.command.ShutdownCommand;
import org.apache.activemq.console.command.StartCommand;
import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;

/**
 * This class launches ActiveMQ under <a href="http://commons.apache.org/daemon/jsvc.html">Apache JSVC</a>
 *
 * @author areese
 *
 */
public class ActiveMQLauncher implements Daemon {
    private List<String> args;

    /**
     *
     */
    public ActiveMQLauncher() {
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.commons.daemon.Daemon#destroy()
     */
    public void destroy() {
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.commons.daemon.Daemon#init(org.apache.commons.daemon.DaemonContext
     * )
     */
    public void init(DaemonContext arg0) throws Exception {
        // we need to save the args we started with.
        args = Arrays.asList(arg0.getArguments());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.commons.daemon.Daemon#start()
     */
    public void start() throws Exception {
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(System.out));

        Command command = new StartCommand();
        command.setCommandContext(context);

        command.execute(args);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.commons.daemon.Daemon#stop()
     */
    public void stop() throws Exception {
        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(System.out));

        Command command = new ShutdownCommand();
        command.setCommandContext(context);

        List<String> tokens = new ArrayList<String>(Arrays.asList(new String[] {
                "--jmxlocal", "--all", }));

        command.execute(tokens);
    }

}
