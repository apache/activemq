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

Writing a Basic JMS Application using:
    - Synchronous Request/Reply
    - Publish/Subscribe
    - javax.jms.TopicRequestor class
    - JMSReplyTo Header

When this program runs, it waits for
messages to the topic "jms.samples.request".
When that message occurs, a response based on the request
is sent back to the "Requestor" specified in the JMSReplyTo header.

This sample replies with a simple text manipulation of the request;
the text is either folded to all UPPERCASE or all lowercase.

Usage:
  java TopicReplier -b <broker:port> -u <username> -p <password> -m <mode>
      -b broker:port points to your message broker
                     Default: tcp://localhost:61616
      -u username    must be unique (but is not checked)
                     Default: SampleReplier
      -p password    password for user (not checked)
                     Default: password
      -m mode        replier mode (uppercase, or lowercase)
                     Default: uppercase

Suggested usage:
  - In a console window with the environment set, start a replier:
       java TopicReplier -u SampleReplier
  - In another console window, start a Requestor:
       java TopicRequestor -u SampleRequestor
  - Enter text in the Requestor window then press Enter.

    The Replier responds with the message in all uppercase characters.
  - Start other TopicRequestors with different user names to see that
    replies are not broadcast to all users. For example:
       java TopicRequestor -u SampleRequestorToo

  - Start other TopicRepliers.
  - See that all repliers are receiving all the messages,(as they should).
  - See the Requestor only receives one response.
       java TopicReplier -u toLower -m lowercase

 */

import org.apache.activemq.*;


public class TopicReplier
    implements javax.jms.MessageListener
{
    private static final String APP_TOPIC = "jms.samples.request";
    private static final String DEFAULT_BROKER_NAME = "tcp://localhost:61616";
    private static final String DEFAULT_USER_NAME = "SampleReplier";
    private static final String DEFAULT_PASSWORD = "password";
    private static final String DEFAULT_MODE = "uppercase";
    private static final int UPPERCASE = 0;
    private static final int LOWERCASE = 1;

    private javax.jms.Connection connect = null;
    private javax.jms.Session session = null;
    private javax.jms.MessageProducer replier = null;

    private int imode = UPPERCASE;

    /** Create JMS client for publishing and subscribing to messages. */
    private void start ( String broker, String username, String password, String mode)
    {
        // Set the operation mode
        imode = (mode.equals("uppercase")) ? UPPERCASE : LOWERCASE;

        // Create a connection.
        try
        {
            javax.jms.ConnectionFactory factory;
            factory = new ActiveMQConnectionFactory(username, password, broker);
            connect = factory.createConnection (username, password);
            session = connect.createSession(true, javax.jms.Session.AUTO_ACKNOWLEDGE);
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("error: Cannot connect to Broker - " + broker);
            jmse.printStackTrace();
            System.exit(1);
        }

        // Create Subscriber to application topics as well as a Publisher
        // to use for JMS replies.
        try
        {
            javax.jms.Topic topic = session.createTopic (APP_TOPIC);
            javax.jms.MessageConsumer subscriber = session.createConsumer(topic);
            subscriber.setMessageListener(this);
            replier = session.createProducer(null);  // Topic will be set for each reply
            // Now that all setup is complete, start the Connection
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
            while ( true )
            {
                  System.out.println ("\nReplier application:\n"
			            					+ "============================\n"
			            					+ "The application user " + username + " connects to the broker at " + DEFAULT_BROKER_NAME + ".\n"
											+ "The application gets requests with JMSReplyTo set on the " + APP_TOPIC + " topic."
											+ "The message is transformed to all uppercase or all lowercase, and then returned to the requestor."
			                                + "The Requestor application displays the result.\n\n"
			                                + "Enter EXIT or press Ctrl+C to close the Replier.\n");
                String s = stdin.readLine();
                if ( s == null || s.equalsIgnoreCase("EXIT"))
                {
                    System.out.println ("\nStopping Replier. Please wait..\n>");
                    exit();
                }
           }
        }
        catch ( java.io.IOException ioe )
        {
            ioe.printStackTrace();
        }
    }

    /**
     * Handle the message.
     * (as specified in the javax.jms.MessageListener interface).
     *
     * IMPORTANT NOTE: We must follow the design paradigm for JMS
     * synchronous requests.  That is, we must:
     *   - get the message
     *   - look for the header specifying JMSReplyTo
     *   - send a reply to the topic specified there.
     * Failing to follow these steps might leave the originator
     * of the request waiting forever.
     *
     * OPTIONAL BEHAVIOR: The following actions taken by the
     * message handler represent good programming style, but are
     * not required by the design paradigm for JMS requests.
     *   - set the JMSCorrelationID (tying the response back to
     *     the original request.
     *   - use transacted session "commit" so receipt of request
     *     won't happen without the reply being sent.
     *
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
                System.out.println( "[Request] " + string );

                // Check for a ReplyTo topic
                javax.jms.Topic replyTopic = (javax.jms.Topic) aMessage.getJMSReplyTo();
                if (replyTopic != null)
                {
                    // Send the modified message back.
                    javax.jms.TextMessage reply =  session.createTextMessage();
                    if (imode == UPPERCASE)
                        reply.setText("Transformed " + string + " to all uppercase: " + string.toUpperCase());
                    else
                        reply.setText("Transformed " + string + " to all lowercase " + string.toLowerCase());
                    reply.setJMSCorrelationID(aMessage.getJMSMessageID());
                    replier.send(replyTopic, reply);
                    session.commit();
                }
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

    /** Cleanup resources cleanly and exit. */
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

        // Values to be read from parameters
        String broker    = DEFAULT_BROKER_NAME;
        String username  = DEFAULT_USER_NAME;
        String password  = DEFAULT_PASSWORD;
        String mode  = DEFAULT_MODE;

        // Check parameters
        for (int i = 0; i < argv.length; i++) {
            String arg = argv[i];


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

            if (arg.equals("-m")) {
                if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                    System.err.println("error: missing mode");
                    System.exit(1);
                }
                mode = argv[++i];
                if (!(mode.equals("uppercase") || mode.equals("lowercase"))) {
                    System.err.println("error: mode must be 'uppercase' or 'lowercase'");
                    System.exit(1);
                }
                continue;
            }

            if (arg.equals("-h")) {
                printUsage();
                System.exit(1);
            }

            // Invalid argument
            System.err.println ("error: unexpected argument: "+arg);
            printUsage();
            System.exit(1);
        }


        // Start the JMS client for the "chat".
        TopicReplier replier = new TopicReplier();
        replier.start (broker, username, password, mode);
    }

    /** Prints the usage. */
    private static void printUsage() {

        StringBuffer use = new StringBuffer();
        use.append("usage: java Replier (options) ...\n\n");
        use.append("options:\n");
        use.append("  -b name:port Specify name:port of broker.\n");
        use.append("               Default broker: "+DEFAULT_BROKER_NAME+"\n");
        use.append("  -u name      Specify unique user name.\n");
        use.append("               Default broker: "+DEFAULT_USER_NAME+"\n");
        use.append("  -p password  Specify password for user.\n");
        use.append("               Default password: "+DEFAULT_PASSWORD+"\n");
        use.append("  -m mode      Replier operating mode - uppercase or lowercase.\n");
        use.append("               Default mode: "+DEFAULT_MODE+"\n");
        use.append("  -h           This help screen.\n");
        System.err.println (use);
    }

}

