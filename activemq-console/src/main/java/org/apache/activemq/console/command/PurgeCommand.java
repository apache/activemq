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
package org.apache.activemq.console.command;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;

import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class PurgeCommand extends AbstractJmxCommand {

    protected String[] helpFile = new String[] {
        "Task Usage: Main purge [browse-options] <destinations>",
        "Description: Delete selected destination's messages that matches the message selector.", 
        "", 
        "Purge Options:",
        "    --msgsel <msgsel1,msglsel2>   Add to the search list messages matched by the query similar to",
        "                                  the messages selector format.",
        "    --jmxurl <url>                Set the JMX URL to connect to.",
        "    --pid <pid>                   Set the pid to connect to (only on Sun JVM).",            
        "    --jmxuser <user>              Set the JMX user used for authenticating.",
        "    --jmxpassword <password>      Set the JMX password used for authenticating.",
        "    --jmxlocal                    Use the local JMX server instead of a remote one.",
        "    --version                     Display the version information.",
        "    -h,-?,--help                  Display the browse broker help information.", 
        "", 
        "Examples:",
        "    Main purge FOO.BAR", 
        "        - Delete all the messages in queue FOO.BAR",

        "    Main purge --msgsel JMSMessageID='*:10',JMSPriority>5 FOO.*", 
        "        - Delete all the messages in the destinations that matches FOO.* and has a JMSMessageID in",
        "          the header field that matches the wildcard *:10, and has a JMSPriority field > 5 in the",
        "          queue FOO.BAR",
        "        * To use wildcard queries, the field must be a string and the query enclosed in ''",
        "",
    };

    private final List<String> queryAddObjects = new ArrayList<String>(10);
    private final List<String> querySubObjects = new ArrayList<String>(10);

    /**
     * Execute the purge command, which allows you to purge the messages in a
     * given JMS destination
     * 
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List<String> tokens) throws Exception {
        try {
            // If there is no queue name specified, let's select all
            if (tokens.isEmpty()) {
                tokens.add("*");
            }

            // Iterate through the queue names
            for (Iterator<String> i = tokens.iterator(); i.hasNext();) {
                List queueList = JmxMBeansUtil.queryMBeans(createJmxConnection(), "Type=Queue,Destination=" + i.next() + ",*");

                for (Iterator j = queueList.iterator(); j.hasNext();) {
                    ObjectName queueName = ((ObjectInstance)j.next()).getObjectName();
                    if (queryAddObjects.isEmpty()) {
                        purgeQueue(queueName);
                    } else {
                        QueueViewMBean proxy = (QueueViewMBean) MBeanServerInvocationHandler.newProxyInstance(createJmxConnection(), queueName, QueueViewMBean.class, true);
                        int removed = 0;
                        for (String remove : queryAddObjects) {
                            removed = proxy.removeMatchingMessages(remove);
                            context.printInfo("Removed: " + removed
                                    + " messages for msgsel" + remove);
                        }
                    }
                }
            }
        } catch (Exception e) {
            context.printException(new RuntimeException("Failed to execute purge task. Reason: " + e));
            throw new Exception(e);
        }
    }

    /**
     * Purge all the messages in the queue
     * 
     * @param queue - ObjectName of the queue to purge
     * @throws Exception
     */
    public void purgeQueue(ObjectName queue) throws Exception {
        context.printInfo("Purging all messages in queue: " + queue.getKeyProperty("Destination"));
        createJmxConnection().invoke(queue, "purge", new Object[] {}, new String[] {});
    }

    /**
     * Handle the --msgsel, --xmsgsel.
     * 
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        // If token is an additive message selector option
        if (token.startsWith("--msgsel")) {

            // If no message selector is specified, or next token is a new
            // option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("Message selector not specified"));
                return;
            }

            StringTokenizer queryTokens = new StringTokenizer((String)tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                queryAddObjects.add(queryTokens.nextToken());
            }
        } else if (token.startsWith("--xmsgsel")) {
            // If token is a substractive message selector option

            // If no message selector is specified, or next token is a new
            // option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("Message selector not specified"));
                return;
            }

            StringTokenizer queryTokens = new StringTokenizer((String)tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                querySubObjects.add(queryTokens.nextToken());
            }

        } else {
            // Let super class handle unknown option
            super.handleOption(token, tokens);
        }
    }

    /**
     * Print the help messages for the browse command
     */
    protected void printHelp() {
        context.printHelp(helpFile);
    }

}
