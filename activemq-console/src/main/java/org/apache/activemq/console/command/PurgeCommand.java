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
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

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
        "    --reset                       After the purge operation, reset the destination statistics.",
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

        "    Main purge --msgsel \"JMSMessageID='*:10',JMSPriority>5\" FOO.*",
        "        - Delete all the messages in the destinations that matches FOO.* and has a JMSMessageID in",
        "          the header field that matches the wildcard *:10, and has a JMSPriority field > 5 in the",
        "          queue FOO.BAR.",
        "          SLQ92 syntax is also supported.",
        "        * To use wildcard queries, the field must be a string and the query enclosed in ''",
        "          Use double quotes \"\" around the entire message selector string.",
        ""
    };

    private final List<String> queryAddObjects = new ArrayList<String>(10);
    private final List<String> querySubObjects = new ArrayList<String>(10);
    private boolean resetStatistics;

    @Override
    public String getName() {
        return "purge";
    }

    @Override
    public String getOneLineDescription() {
        return "Delete selected destination's messages that matches the message selector";
    }

    /**
     * Execute the purge command, which allows you to purge the messages in a
     * given JMS destination
     *
     * @param tokens - command arguments
     * @throws Exception
     */
    @Override
    protected void runTask(List<String> tokens) throws Exception {
        // If there is no queue name specified, let's select all
        if (tokens.isEmpty()) {
            tokens.add("*");
        }

        // Iterate through the queue names
        for (Iterator<String> i = tokens.iterator(); i.hasNext(); ) {
            List queueList = JmxMBeansUtil.queryMBeans(createJmxConnection(), "type=Broker,brokerName=*,destinationType=Queue,destinationName=" + i.next());

            for (Iterator j = queueList.iterator(); j.hasNext(); ) {
                ObjectName queueName = ((ObjectInstance) j.next()).getObjectName();
                if (queryAddObjects.isEmpty()) {
                    purgeQueue(queueName);
                } else {

                    QueueViewMBean proxy = MBeanServerInvocationHandler.
                            newProxyInstance(createJmxConnection(),
                                    queueName,
                                    QueueViewMBean.class,
                                    true);
                    int removed = 0;

                    // AMQ-3404: We support two syntaxes for the message
                    // selector query:
                    // 1) AMQ specific:
                    //    "JMSPriority>2,MyHeader='Foo'"
                    //
                    // 2) SQL-92 syntax:
                    //    "(JMSPriority>2) AND (MyHeader='Foo')"
                    //
                    // If syntax style 1) is used, the comma separated
                    // criterias are broken into List<String> elements.
                    // We then need to construct the SQL-92 query out of
                    // this list.

                    String sqlQuery = convertToSQL92(queryAddObjects);
                    removed = proxy.removeMatchingMessages(sqlQuery);
                    context.printInfo("Removed: " + removed
                            + " messages for message selector " + sqlQuery);

                    if (resetStatistics) {
                        proxy.resetStatistics();
                    }
                }
            }
        }
    }


    /**
     * Purge all the messages in the queue
     *
     * @param queue - ObjectName of the queue to purge
     * @throws Exception
     */
    public void purgeQueue(ObjectName queue) throws Exception {
        context.printInfo("Purging all messages in queue: " + queue.getKeyProperty("destinationName"));
        createJmxConnection().invoke(queue, "purge", new Object[] {}, new String[] {});
        if (resetStatistics) {
            createJmxConnection().invoke(queue, "resetStatistics", new Object[] {}, new String[] {});
        }
    }

    /**
     * Handle the --msgsel, --xmsgsel.
     *
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    @Override
    protected void handleOption(String token, List<String> tokens) throws Exception {
        // If token is an additive message selector option
        if (token.startsWith("--msgsel")) {

            // If no message selector is specified, or next token is a new
            // option
            if (tokens.isEmpty() || tokens.get(0).startsWith("-")) {
                context.printException(new IllegalArgumentException("Message selector not specified"));
                return;
            }

            StringTokenizer queryTokens = new StringTokenizer(tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                queryAddObjects.add(queryTokens.nextToken());
            }
        } else if (token.startsWith("--xmsgsel")) {
            // If token is a substractive message selector option

            // If no message selector is specified, or next token is a new
            // option
            if (tokens.isEmpty() || tokens.get(0).startsWith("-")) {
                context.printException(new IllegalArgumentException("Message selector not specified"));
                return;
            }

            StringTokenizer queryTokens = new StringTokenizer(tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (queryTokens.hasMoreTokens()) {
                querySubObjects.add(queryTokens.nextToken());
            }
        } else if (token.startsWith("--reset")) {
            resetStatistics = true;
        } else {
            // Let super class handle unknown option
            super.handleOption(token, tokens);
        }
    }

    /**
     * Converts the message selector as provided on command line
     * argument to activem-admin into an SQL-92 conform string.
     * E.g.
     *   "JMSMessageID='*:10',JMSPriority>5"
     * gets converted into
     *   "(JMSMessageID='%:10') AND (JMSPriority>5)"
     *
     * @param tokens - List of message selector query parameters
     * @return SQL-92 string of that query.
     */
    public String convertToSQL92(List<String> tokens) {
        StringBuilder selector = new StringBuilder();

        boolean isFirstToken = true;
        for (Iterator i = tokens.iterator(); i.hasNext(); ) {
            String token = i.next().toString();
            if (token.matches("^[^=]*='.*[\\*\\?].*'$")) {
                token = token.replace('?', '_')
                        .replace('*', '%')
                        .replaceFirst("=", " LIKE ");
            }
            if (isFirstToken) {
                isFirstToken = false;
            } else {
                selector.append(" AND ");
            }
            selector.append('(')
                    .append(token)
                    .append(')');
        }
        return selector.toString();
    }

    /**
     * Print the help messages for the browse command
     */
    @Override
    protected void printHelp() {
        context.printHelp(helpFile);
    }

}
