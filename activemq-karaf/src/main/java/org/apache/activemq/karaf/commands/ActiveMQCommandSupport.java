/*
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
package org.apache.activemq.karaf.commands;

import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.command.AbstractJmxCommand;
import org.apache.activemq.console.command.Command;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.apache.felix.gogo.commands.Argument;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import java.util.ArrayList;

/**
 * @version $Rev: 960482 $ $Date: 2010-07-05 10:28:33 +0200 (Mon, 05 Jul 2010) $
 */
public class ActiveMQCommandSupport extends OsgiCommandSupport {

    private Command command;

    @Argument(index=0, multiValued=true, required=true)
    private ArrayList<String> arguments = new ArrayList<String>();

    protected Object doExecute() throws Exception {
        CommandContext context2 = new CommandContext();
        context2.setFormatter(new CommandShellOutputFormatter(System.out));
        Command currentCommand = command.getClass().newInstance();

        try {
            currentCommand.setCommandContext(context2);
            // must be added first
            if (command instanceof AbstractJmxCommand) {
                arguments.add(0, "--jmxlocal");
            }
            currentCommand.execute(arguments);
            return null;
        } catch (Throwable e) {
            Throwable cur = e;
            while (cur.getCause() != null) {
                cur = cur.getCause();
            }
            if (cur instanceof java.net.ConnectException) {
                context2
                    .print("\n"
                        + "Could not connect to JMX server.  This command requires that the remote JMX server be enabled.\n"
                        + "This is typically done by adding the following JVM arguments: \n"
                        + "   -Dcom.sun.management.jmxremote.port=1099 -Dcom.sun.management.jmxremote.authenticate=false \n"
                        + "   -Dcom.sun.management.jmxremote.ssl=false \n" + "\n"
                        + "The connection error was: " + cur + "\n");
            } else {
                if (e instanceof Exception) {
                    throw (Exception)e;
                } else {
                    throw new RuntimeException(e);
                }

            }
        }
        return null;

    }

    /**
     * @return the description of the command.
     */
    public String description() {
        return command.getOneLineDescription();
    }

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public static String[] toStringArray(Object args[]) {
        String strings[] = new String[args.length];
        for(int i = 0; i < args.length; i++) {
            strings[i] = String.valueOf(args[i]);
        }
        return strings;
    }
}
