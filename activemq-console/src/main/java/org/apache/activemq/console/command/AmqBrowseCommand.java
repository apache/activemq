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

import javax.jms.Destination;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.console.util.AmqMessagesUtil;

public class AmqBrowseCommand extends AbstractAmqCommand {

    public static final String QUEUE_PREFIX = "queue:";
    public static final String TOPIC_PREFIX = "topic:";

    public static final String VIEW_GROUP_HEADER = "header:";
    public static final String VIEW_GROUP_CUSTOM = "custom:";
    public static final String VIEW_GROUP_BODY = "body:";

    protected String[] helpFile = new String[] {
        "Task Usage: Main browse [browse-options] <destinations>",
        "Description: Display selected destination's messages.",
        "",
        "Browse Options:",
        "    --amqurl <url>                Set the broker URL to connect to. Default tcp://localhost:61616",
        "    --msgsel <msgsel1,msglsel2>   Add to the search list messages matched by the query similar to",
        "                                  the messages selector format.",
        "    --factory <className>         Load className as the javax.jms.ConnectionFactory to use for creating connections.",
        "    --passwordFactory <className> Load className as the org.apache.activemq.console.command.PasswordFactory",
        "                                  for retrieving the password from a keystore.",
        "    --user <username>             Username to use for JMS connections.",
        "    --password <password>         Password to use for JMS connections.",
        "    -V<header|custom|body>        Predefined view that allows you to view the message header, custom",
        "                                  message header, or the message body.",
        "    --view <attr1>,<attr2>,...    Select the specific attribute of the message to view.",
        "    --version                     Display the version information.",
        "    -h,-?,--help                  Display the browse broker help information.",
        "",
        "Examples:",
        "    Main browse --amqurl tcp://localhost:61616 FOO.BAR",
        "        - Print the message header, custom message header, and message body of all messages in the",
        "          queue FOO.BAR",
        "",
        "    Main browse --amqurl tcp://localhost:61616 -Vheader,body queue:FOO.BAR",
        "        - Print only the message header and message body of all messages in the queue FOO.BAR",
        "",
        "    Main browse --amqurl tcp://localhost:61616 -Vheader --view custom:MyField queue:FOO.BAR",
        "        - Print the message header and the custom field 'MyField' of all messages in the queue FOO.BAR",
        "",
        "    Main browse --amqurl tcp://localhost:61616 --msgsel \"JMSMessageID='*:10',JMSPriority>5\" FOO.BAR",
        "        - Print all the message fields that has a JMSMessageID in the header field that matches the",
        "          wildcard *:10, and has a JMSPriority field > 5 in the queue FOO.BAR",
        "        * To use wildcard queries, the field must be a string and the query enclosed in ''",
        "",
        "    Main browse --amqurl tcp://localhost:61616 --user someUser --password somePass FOO.BAR",
        "        - Print the message header, custom message header, and message body of all messages in the",
        "          queue FOO.BAR, using someUser as the user name, and somePass as the password",
        "",
        "    Main browse --amqurl tcp://localhost:61616 --user someUser --password somePass --factory org.apache.activemq.ActiveMQConnectionFactory --passwordFactory org.apache.activemq.AMQPasswordFactory FOO.BAR",
        "        - Print the message header, custom message header, and message body of all messages in the",
        "          queue FOO.BAR, using someUser as the user name, org.apache.activemq.AMQFactorySubClass to create JMS connections,",
        "          and org.apache.activemq.console.command.DefaultPasswordFactory to turn somePass into the password to be used.",
        "",
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
        return "Display selected messages in a specified destination.";
    }

    /**
     * Execute the browse command, which allows you to browse the messages in a
     * given JMS destination
     *
     * @param tokens - command arguments
     * @throws Exception
     */
    protected void runTask(List tokens) throws Exception {
        try {
            // If no destination specified
            if (tokens.isEmpty()) {
                context.printException(new IllegalArgumentException("No JMS destination specified."));
                return;
            }

            // If no broker url specified
            if (getBrokerUrl() == null) {
                setBrokerUrl(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
            }

            // Display the messages for each destination
            for (Iterator i = tokens.iterator(); i.hasNext();) {
                String destName = (String)i.next();
                Destination dest;

                // If destination has been explicitly specified as a queue
                if (destName.startsWith(QUEUE_PREFIX)) {
                    dest = new ActiveMQQueue(destName.substring(QUEUE_PREFIX.length()));

                    // If destination has been explicitly specified as a topic
                } else if (destName.startsWith(TOPIC_PREFIX)) {
                    dest = new ActiveMQTopic(destName.substring(TOPIC_PREFIX.length()));

                    // By default destination is assumed to be a queue
                } else {
                    dest = new ActiveMQQueue(destName);
                }

                // Query for the messages to view
                List addMsgs = AmqMessagesUtil.getMessages(getConnectionFactory(), dest, queryAddObjects);

                // Query for the messages to remove from view
                if (querySubObjects.size() > 0) {
                    List subMsgs = AmqMessagesUtil.getMessages(getConnectionFactory(), dest, querySubObjects);
                    addMsgs.removeAll(subMsgs);
                }

                // Display the messages
                context.printMessage(AmqMessagesUtil.filterMessagesView(addMsgs, groupViews, queryViews));
            }

        } catch (Exception exception) {
            handleException(exception, getBrokerUrl().toString());
        }
    }

    /**
     * Handle the --msgsel, --xmsgsel, --view, -V options.
     *
     * @param token - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List tokens) throws Exception {

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

            // If no message selector is specified, or next token is a new option
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
