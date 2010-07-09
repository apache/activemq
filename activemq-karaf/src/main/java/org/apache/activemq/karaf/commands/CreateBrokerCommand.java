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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.felix.gogo.commands.Option;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * @version $Rev: 960482 $ $Date: 2010-07-05 10:28:33 +0200 (Mon, 05 Jul 2010) $
 */
 @Command(scope="activemq", name="create-broker", description="Creates a broker instance.")
public class CreateBrokerCommand extends OsgiCommandSupport {
    
    @Option(name = "-n", aliases = {"--name"}, description = "The name of the broker (defaults to localhost).")
    private String name = "localhost";
    @Option(name = "-t", aliases = {"--type"}, description = "type of configuration to be used: spring or blueprint (defaults to spring)")
    private String type = "spring";

    /*
     * (non-Javadoc)
     * @see
     * org.apache.karaf.shell.console.OsgiCommandSupport#doExecute()
     */
    protected Object doExecute() throws Exception {

        try {
            String name = getName();
            File base = new File(System.getProperty("karaf.base"));
            File deploy = new File(base, "deploy");

            HashMap<String, String> props = new HashMap<String, String>();
            props.put("${name}", name);

            mkdir(deploy);
            File configFile = new File(deploy, name + "-broker.xml");
            
            if (!type.equalsIgnoreCase("spring") && !type.equalsIgnoreCase("blueprint")) {
                System.out.println("@green Unknown type '" + type + "' Using spring by default");
                type = "spring";
            }
            
            copyFilteredResourceTo(configFile, type.toLowerCase() + ".xml", props);

            System.out.println("");
            System.out.println("Default ActiveMQ Broker (" + name + ") configuration file created at: "
                           + configFile.getPath());
            System.out.println("Please review the configuration and modify to suite your needs.  ");
            System.out.println("");

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        return 0;
    }

    private void copyFilteredResourceTo(File outFile, String resource, HashMap<String, String> props)
        throws Exception {
        if (!outFile.exists()) {
            System.out.println("Creating file: @|green " + outFile.getPath() + "|");
            InputStream is = CreateBrokerCommand.class.getResourceAsStream(resource);
            try {
                // Read it line at a time so that we can use the platform line
                // ending when we write it out.
                PrintStream out = new PrintStream(new FileOutputStream(outFile));
                try {
                    Scanner scanner = new Scanner(is);
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        line = filter(line, props);
                        out.println(line);
                    }
                } finally {
                    safeClose(out);
                }
            } finally {
                safeClose(is);
            }
        } else {
            System.out.println("@|red File already exists|. Move it out of the way if you want it re-created: "
                           + outFile.getPath() + "");
        }
    }

    private void safeClose(InputStream is) throws IOException {
        if (is == null)
            return;
        try {
            is.close();
        } catch (Throwable ignore) {
        }
    }

    private void safeClose(OutputStream is) throws IOException {
        if (is == null)
            return;
        try {
            is.close();
        } catch (Throwable ignore) {
        }
    }

    private String filter(String line, HashMap<String, String> props) {
        for (Map.Entry<String, String> i : props.entrySet()) {
            int p1;
            while ((p1 = line.indexOf(i.getKey())) >= 0) {
                String l1 = line.substring(0, p1);
                String l2 = line.substring(p1 + i.getKey().length());
                line = l1 + i.getValue() + l2;
            }
        }
        return line;
    }

    private void mkdir(File file) {
        if (!file.exists()) {
            System.out.println("Creating missing directory: @|green " + file.getPath() + "|");
            file.mkdirs();
        }
    }

    public String getName() {
        if (name == null) {
            File base = new File(System.getProperty("karaf.base"));
            name = base.getName();
        }
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
