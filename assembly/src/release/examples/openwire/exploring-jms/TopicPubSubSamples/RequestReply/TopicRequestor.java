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

When this program runs, it reads input from System.in
and then sends the text as a message to the topic
"progress.samples.request".

A "Replier" class should be waiting for the request.
It will reply with a message.

NOTE: You must run the TopicReplier first.
(Otherwise the syncronous request will block forever.)

Usage:
  java TopicRequestor -b <broker:port> -u <username> -p <password>
      -b broker:port points to your message broker
                     Default: tcp://localhost:61616
      -u username    must be unique (but is not checked)
                     Default: SampleRequestor
      -p password    password for user (not checked)
                     Default: password

Suggested demonstration:
  - In a console window with the environment set,
    start a copy of the Replier. For example:
       java TopicReplier -u SampleReplier
  - In another console window, start a Requestor.
    For example:
       java TopicRequestor -u SampleRequestor
  - Enter text in the Requestor window then press Enter.
  
    The Replier responds with the message in all uppercase characters.
  - Start other Requestors with different user names to see that
    replies are not broadcast to all users. For example:
       java TopicRequestor -u SampleRequestorToo

  - Start other Repliers.
  - See that all repliers are receiving all the messages,(as they should).
  - See the Requestor only receives one response.
       java TopicReplier -u toLower -m lowercase
*/
import org.apache.activemq.*;


public class TopicRequestor
{
    private static final String APP_TOPIC = "jms.samples.request";
    private static final String DEFAULT_BROKER_NAME = "tcp://localhost:61616";
    private static final String DEFAULT_USER_NAME = "SampleRequestor";
    private static final String DEFAULT_PASSWORD = "password";

    private javax.jms.TopicConnection connect = null;
    private javax.jms.TopicSession session = null;

    /** Create JMS client for publishing and subscribing to messages. */
    private void start ( String broker, String username, String password)
    {
        // Create a connection.
        try
        {
            javax.jms.TopicConnectionFactory factory;
            factory = new ActiveMQConnectionFactory(username, password, broker);
            connect = factory.createTopicConnection (username, password);
            session = connect.createTopicSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("error: Cannot connect to Broker - " + broker);
            jmse.printStackTrace();
            System.exit(1);
        }

        // Create Topic for all requests.  TopicRequestor will be created
        // as needed.
        javax.jms.Topic topic = null;
        try
        {
            topic = session.createTopic (APP_TOPIC);
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
            System.out.println ("\nRequestor application:\n"
			            					+ "============================\n"
			            					+ "The application user " + username + " connects to the broker at " + DEFAULT_BROKER_NAME + ".\n"
											+ "The application uses a TopicRequestor to on the " + APP_TOPIC + " topic."
											+ "The Replier application gets the message, and transforms it."
			                                + "The Requestor application displays the result.\n\n"
			                                + "Type some mixed case text, and then press Enter to make a request.\n");
            while ( true )
            {
                String s = stdin.readLine();

                if ( s == null )
                    exit();
                else if ( s.length() > 0 )
                {
                    javax.jms.TextMessage msg = session.createTextMessage();
                    msg.setText( username + ": " + s );
                    // Instead of publishing, we will use a TopicRequestor.
                    javax.jms.TopicRequestor requestor = new javax.jms.TopicRequestor(session, topic);
                    javax.jms.Message response = requestor.request(msg);
                    // The message should be a TextMessage.  Just report it.
                    javax.jms.TextMessage textMessage = (javax.jms.TextMessage) response;
                    System.out.println( "[Reply] " + textMessage.getText() );
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
        TopicRequestor requestor = new TopicRequestor();
        requestor.start (broker, username, password);

    }

    /** Prints the usage. */
    private static void printUsage() {

        StringBuffer use = new StringBuffer();
        use.append("usage: java Requestor (options) ...\n\n");
        use.append("options:\n");
        use.append("  -b name:port Specify name:port of broker.\n");
        use.append("               Default broker: "+DEFAULT_BROKER_NAME+"\n");
        use.append("  -u name      Specify unique user name.\n");
        use.append("               Default broker: "+DEFAULT_USER_NAME+"\n");
        use.append("  -p password  Specify password for user.\n");
        use.append("               Default password: "+DEFAULT_PASSWORD+"\n");
        use.append("  -h           This help screen.\n");
        System.err.println (use);
    }

}

