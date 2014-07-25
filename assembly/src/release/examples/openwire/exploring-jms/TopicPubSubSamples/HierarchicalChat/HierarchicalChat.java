/*
 * © 2001-2009, Progress Software Corporation and/or its subsidiaries or affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
Sample Application

Writing a JMS Application using Publish and Subscribe with
Hierarchical Topics

This sample publishes and subscribes to specified topic nodes.
Text you enter is published and then received by all subscribers to
the that topic on the specified broker.

Usage:
  java HierarchicalChat -b <broker:port> -u <username> -p <password> -t <pubTopicname> -s <subTopicname>
      -b broker:port        points to a message broker
                            Default: tcp://localhost:61616
      -u username           must be unique (but is not checked)
      -p password           password for user (not checked)
      -t pubTopicname     	name of topic to which to publish
                            Default: jms.samples.hierarchicalchat
      -s subTopicname	    name of topic to which to subscribe
                            Default: jms.samples.*

Suggested demonstration:
  - In separate console windows with the environment set,
    start instances of the application
    under unique user names.
    For example:
       java HierarchicalChat -u SALES -t sales -s sales.*
       java HierarchicalChat -u USA -t sales.usa -s sales.usa
  - Enter text in the USA console window and then press Enter
    to publish the message.
  - Note that messages published from the SALES console window
    to the sales topic are not seen by the USA user listening
    to messages on the sales.usa topic
  - Message published to the sales.usa are received by the SALES
    user listening to sales.*
  - Stop a session by pressing CTRL+C in its console window.

*/

import org.apache.activemq.*;


public class HierarchicalChat
    implements javax.jms.MessageListener
{
    private static final String DEFAULT_PUBLISHER_TOPIC = "jms.samples.hierarchicalchat";
    private static final String DEFAULT_SUBSCRIBER_TOPIC = "jms.samples.*";
    private static final String DEFAULT_SUBSCRIBER_ROOT = "jms.samples";
    private static final String DEFAULT_BROKER_NAME = "tcp://localhost:61616";

    private static final String DEFAULT_PASSWORD = "password";

    private javax.jms.Connection connect = null;
    private javax.jms.Session pubSession = null;
    private javax.jms.Session subSession = null;
    private javax.jms.MessageProducer publisher = null;

    /** Create JMS client for publishing and subscribing to messages. */
    private void chatter( String broker, String username, String password, String pubTopicname, String subTopicname)
    {

        // Create a connection.
        try
        {
            javax.jms.ConnectionFactory factory;
            factory = new ActiveMQConnectionFactory(username, password, broker);
            connect = factory.createConnection (username, password);
            pubSession = connect.createSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
            subSession = connect.createSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("error: Cannot connect to Broker - " + broker);
            jmse.printStackTrace();
            System.exit(1);
        }

        // Create Publisher and Subscriber to 'chat' topics
        // Note that the publish and subscribe topics are different.
        try
        {
            javax.jms.Topic subscriberTopic = pubSession.createTopic (subTopicname);
            javax.jms.MessageConsumer subscriber = subSession.createConsumer(subscriberTopic, null, false);
            subscriber.setMessageListener(this);
            javax.jms.Topic publisherTopic = pubSession.createTopic (pubTopicname);
            publisher = pubSession.createProducer(publisherTopic);
            // Now that setup is complete, start the Connection
            connect.start();
        }
        catch (javax.jms.JMSException jmse)
        {
            jmse.printStackTrace();
        }
        try
        {
            // Read all standard input and send it as a message.
            java.io.BufferedReader stdin =
                new java.io.BufferedReader( new java.io.InputStreamReader( System.in ) );
                        System.out.println("\nHierarchicalChat application:\n"
			            					+ "============================\n"
			            					+ "The application user " + username + " connects to the broker at " + DEFAULT_BROKER_NAME + ".\n"
											+ "The application will publish messages to the " + DEFAULT_PUBLISHER_TOPIC + " topic." + ".\n"
			                                + "The application also subscribes to topics using the wildcard syntax " + DEFAULT_SUBSCRIBER_TOPIC 
											+ " so that it can receive all messages to " + DEFAULT_SUBSCRIBER_ROOT + " and its subtopics.\n\n"
			                                + "Type some text, and then press Enter to publish a TextMesssage from " + username + ".\n");
            while ( true )
            {
                String s = stdin.readLine();

                if ( s == null )
                    exit();
                else if ( s.length() > 0 )
                {
                    javax.jms.TextMessage msg = pubSession.createTextMessage();
                    msg.setText( username + ": " + s );
                    publisher.send( msg );
                }
            }
        }
        catch ( java.io.IOException ioe )
        {
            ioe.printStackTrace();
        }
        catch ( javax.jms.JMSException jmse )
        {
            jmse.printStackTrace();
        }
    }

    /**
     * Handle the message
     * (as specified in the javax.jms.MessageListener interface).
     */
    public void onMessage( javax.jms.Message aMessage)
    {
        try
        {
            // Cast the message as a text message.
            javax.jms.TextMessage textMessage = (javax.jms.TextMessage) aMessage;

            // This handler reads a single String from the
            // message and prints it to the standard output.
            try
            {
                String string = textMessage.getText();
                System.out.println( string );
            }
            catch (javax.jms.JMSException jmse)
            {
                jmse.printStackTrace();
            }
        }
        catch (java.lang.RuntimeException rte)
        {
            rte.printStackTrace();
        }
    }

    /** Cleanup resources and then exit. */
    private void exit()
    {
        try
        {
            connect.close();
        }
        catch (javax.jms.JMSException jmse)
        {
            jmse.printStackTrace();
        }

        System.exit(0);
    }

    //
    // NOTE: the remainder of this sample deals with reading arguments
    // and does not utilize any JMS classes or code.
    //

    /** Main program entry point. */
    public static void main(String argv[]) {

        // Is there anything to do?
        if (argv.length == 0) {
            printUsage();
            System.exit(1);
        }

        // Values to be read from parameters
        String broker    = DEFAULT_BROKER_NAME;
        String username  = null;
        String password  = DEFAULT_PASSWORD;
        String pubTopicname = DEFAULT_PUBLISHER_TOPIC;
        String subTopicname = DEFAULT_SUBSCRIBER_TOPIC;

        // Check parameters
        for (int i = 0; i < argv.length; i++) {
            String arg = argv[i];

            // Options
            if (!arg.startsWith("-")) {
                System.err.println ("error: unexpected argument - "+arg);
                printUsage();
                System.exit(1);
            }
            else {
                if (arg.equals("-b")) {
                    if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                        System.err.println("error: missing broker name:port");
                        System.exit(1);
                    }
                    broker = argv[++i];
                    continue;
                }

                if (arg.equals("-u")) {
                    if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                        System.err.println("error: missing user name");
                        System.exit(1);
                    }
                    username = argv[++i];
                    continue;
                }

                if (arg.equals("-p")) {
                    if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                        System.err.println("error: missing password");
                        System.exit(1);
                    }
                    password = argv[++i];
                    continue;
                }
                if (arg.equals("-t")) {
                    if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                        System.err.println("error: missing publisher topic name");
                        System.exit(1);
                    }
                    pubTopicname = argv[++i];
                    continue;
                }

                if (arg.equals("-s")) {
                    if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                        System.err.println("error: missing subscriber topic name");
                        System.exit(1);
                    }
                    subTopicname = argv[++i];
                    continue;
                }

                if (arg.equals("-h")) {
                    printUsage();
                    System.exit(1);
                }
            }
        }

        // Check values read in.
        if (username == null) {
            System.err.println ("error: user name must be supplied");
            printUsage();
        }

        // Start the JMS client for the "chat".
        HierarchicalChat chat = new HierarchicalChat();
        chat.chatter (broker, username, password, pubTopicname, subTopicname);

    }

    /** Prints the usage. */
    private static void printUsage() {

        StringBuffer use = new StringBuffer();
        use.append("usage: java HierarchicalChat (options) ...\n\n");
        use.append("options:\n");
        use.append("  -b name:port          Specify name:port of broker.\n");
        use.append("                        Default broker: "+DEFAULT_BROKER_NAME+"\n");
        use.append("  -u name               Specify unique user name. (Required)\n");
        use.append("  -p password           Specify password for user.\n");
        use.append("                        Default password: "+DEFAULT_PASSWORD+"\n");
        use.append("  -t pubTopicname       name of topic to which to publish.\n");
        use.append("                        Default publisher topic name: "+DEFAULT_PUBLISHER_TOPIC+"\n");
        use.append("  -s subTopicname       Specify subscriber topic name.\n");
        use.append("                        name of topic to which to subscribe: "+DEFAULT_SUBSCRIBER_TOPIC+"\n");
        use.append("  -h                    This help screen.\n");
        System.err.println (use);
    }

}

