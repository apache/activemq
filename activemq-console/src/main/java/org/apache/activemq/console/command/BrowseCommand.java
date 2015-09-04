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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import javax.management.ObjectInstance;

import org.apache.activemq.console.util.AmqMessagesUtil;
import org.apache.activemq.console.util.JmxMBeansUtil;

public class BrowseCommand extends AbstractJmxCommand {

    public static final String QUEUE_PREFIX = "queue:";
    public static final String TOPIC_PREFIX = "topic:";

    public static final String VIEW_GROUP_HEADER = "header:";
    public static final String VIEW_GROUP_CUSTOM = "custom:";
    public static final String VIEW_GROUP_BODY = "body:";

    protected String[] helpFile = new String[] {
        "Task Usage: Main browse [browse-options] <destinations>", "Description: Display selected destination's messages.", 
        "", 
        "Browse Options:",
        "    --msgsel <msgsel1,msglsel2>   Add to the search list messages matched by the query similar to", 
        "                                  the messages selector format.",
        "    -V<header|custom|body>        Predefined view that allows you to view the message header, custom", 
        "                                  message header, or the message body.",
        "    --view <attr1>,<attr2>,...    Select the specific attribute of the message to view.", 
        "    --jmxurl <url>                Set the JMX URL to connect to.",
        "    --pid <pid>                   Set the pid to connect to (only on Sun JVM).",            
        "    --jmxuser <user>              Set the JMX user used for authenticating.",
        "    --jmxpassword <password>      Set the JMX password used for authenticating.",
        "    --jmxlocal                    Use the local JMX server instead of a remote one.",
        "    --version                     Display the version information.",
        "    -h,-?,--help                  Display the browse broker help information.", 
        "", 
        "Examples:",
        "    Main browse FOO.BAR", 
        "        - Print the message header, custom message header, and message body of all messages in the", 
        "          queue FOO.BAR",
        "",
        "    Main browse -Vheader,body queue:FOO.BAR",
        "        - Print only the message header and message body of all messages in the queue FOO.BAR",
        "",
        "    Main browse -Vheader --view custom:MyField queue:FOO.BAR",
        "        - Print the message header and the custom field 'MyField' of all messages in the queue FOO.BAR",
        "",
        "    Main browse --msgsel \"JMSMessageID='*:10',JMSPriority>5\" FOO.BAR", 
        "        - Print all the message fields that has a JMSMessageID in the header field that matches the",
        "          wildcard *:10, and has a JMSPriority field > 5 in the queue FOO.BAR.", 
        "          SLQ92 syntax is also supported.",
        "        * To use wildcard queries, the field must be a string and the query enclosed in ''", 
        "          Use double quotes \"\" around the entire message selector string.",
        ""
    };

    private final List<String> queryAddObjects = new ArrayList<String>(10);
    private final List<String> querySubObjects = new ArrayList<String>(10);
    private final Set<String> groupViews = new HashSet<String>(10);
    private final Set queryViews = new HashSet(10);

    @Override
    public String getName() {
        return "browse";
    }

    @Override
    public String getOneLineDescription() {
        return "Used to browse a destination";
    }

    /**
     * Execute the browse command, which allows you to browse the messages in a
     * given JMS destination
     * 
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List<String> tokens) throws Exception {
        // If there is no queue name specified, let's select all
        if (tokens.isEmpty()) {
            tokens.add("*");
        }

        // Iterate through the queue names
        for (Iterator<String> i = tokens.iterator(); i.hasNext(); ) {
            List queueList = JmxMBeansUtil.queryMBeans(createJmxConnection(), "Type=Queue,Destination=" + i.next() + ",*");

            // Iterate through the queue result
            for (Iterator j = queueList.iterator(); j.hasNext(); ) {
                List messages = JmxMBeansUtil.createMessageQueryFilter(createJmxConnection(), ((ObjectInstance) j.next()).getObjectName()).query(queryAddObjects);
                context.printMessage(JmxMBeansUtil.filterMessagesView(messages, groupViews, queryViews));
            }
        }
    }

    /**
     * Handle the --msgsel, --xmsgsel, --view, -V options.
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

        } else if (token.startsWith("--view")) {
            // If token is a view option

            // If no view specified, or next token is a new option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                context.printException(new IllegalArgumentException("Attributes to view not specified"));
                return;
            }

            // Add the attributes to view
            StringTokenizer viewTokens = new StringTokenizer((String)tokens.remove(0), COMMAND_OPTION_DELIMETER);
            while (viewTokens.hasMoreTokens()) {
                String viewToken = viewTokens.nextToken();

                // If view is explicitly specified to belong to the JMS header
                if (viewToken.equals(VIEW_GROUP_HEADER)) {
                    queryViews.add(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + viewToken.substring(VIEW_GROUP_HEADER.length()));

                    // If view is explicitly specified to belong to the JMS
                    // custom header
                } else if (viewToken.equals(VIEW_GROUP_CUSTOM)) {
                    queryViews.add(AmqMessagesUtil.JMS_MESSAGE_CUSTOM_PREFIX + viewToken.substring(VIEW_GROUP_CUSTOM.length()));

                    // If view is explicitly specified to belong to the JMS body
                } else if (viewToken.equals(VIEW_GROUP_BODY)) {
                    queryViews.add(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + viewToken.substring(VIEW_GROUP_BODY.length()));

                    // If no view explicitly specified, let's check the view for
                    // each group
                } else {
                    queryViews.add(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX + viewToken);
                    queryViews.add(AmqMessagesUtil.JMS_MESSAGE_CUSTOM_PREFIX + viewToken);
                    queryViews.add(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX + viewToken);
                }
            }
        } else if (token.startsWith("-V")) {
            // If token is a predefined group view option
            String viewGroup = token.substring(2);
            // If option is a header group view
            if (viewGroup.equals("header")) {
                groupViews.add(AmqMessagesUtil.JMS_MESSAGE_HEADER_PREFIX);

                // If option is a custom header group view
            } else if (viewGroup.equals("custom")) {
                groupViews.add(AmqMessagesUtil.JMS_MESSAGE_CUSTOM_PREFIX);

                // If option is a body group view
            } else if (viewGroup.equals("body")) {
                groupViews.add(AmqMessagesUtil.JMS_MESSAGE_BODY_PREFIX);

                // Unknown group view
            } else {
                context.printInfo("Unknown group view: " + viewGroup + ". Ignoring group view option.");
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
