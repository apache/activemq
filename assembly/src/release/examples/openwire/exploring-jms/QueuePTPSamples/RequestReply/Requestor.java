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

/*
Copyright 2001-2008, Progress Software Corporation -  All Rights Reserved

Sample Application

Writing a Basic JMS Application with Point-to-Point Queues,
using:
    - Synchronous Request/Reply
    - javax.jms.QueueRequestor class
    - JMSReplyTo Header

When this program runs, it reads input from System.in
and then sends the text as a message to the queue, "Q1"
(by default).

A "Replier" class should be waiting for the request.
It will reply with a message.

NOTE: Unlike the Publish-Subscribe example, you need
not run the Replier first.  However, this Requestor
will block until the Replier is started to service the queue.

Usage:
  java Requestor -b <broker:port> -u <username> -p <password> -qs <queue>
      -b broker:port points to your message broker
                     Default: tcp://localhost:61616
      -u username    must be unique (but is not checked)
                     Default: SampleRequestor
      -p password    password for user (not checked)
                     Default: password
      -qs queue      name of queue for sending requests
                     Default: SampleQ1

Suggested demonstration:
  - In a console window with the environment set,
    start a copy of the Replier. For example:
       java Replier -u SampleQReplier
  - In another console window, start a Requestor.
    For example:
       java Requestor -u SampleQRequestor
  - Enter text in the Requestor window then press Enter.
    The Replier responds with the message in all uppercase characters.
  - Start other Requestors with different user names to see that
    replies are not broadcast to all users. For example:
       java Requestor -u SampleRequestorFoo
  - Start other Repliers.
  - See that only one replier is receiving messages,(as it should).
  - See the Requestor only receives one response.
       java Replier -u toLower -m lowercase

*/
import org.apache.activemq.*;


public class Requestor
{
    private static final String DEFAULT_BROKER_NAME = "tcp://localhost:61616";
    private static final String DEFAULT_USER_NAME = "SampleRequestor";
    private static final String DEFAULT_PASSWORD = "password";
    private static final String DEFAULT_QUEUE = "Q1";

    private javax.jms.QueueConnection connect = null;
    private javax.jms.QueueSession session = null;
    private javax.jms.QueueRequestor requestor = null;

    /** Create JMS client for sending messages. */
    private void start ( String broker, String username, String password, String sQueue)
    {
        // Create a connection.
        try
        {
            javax.jms.QueueConnectionFactory factory;
            factory = new ActiveMQConnectionFactory(username, password, broker);
            connect = factory.createQueueConnection (username, password);
            session = connect.createQueueSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("error: Cannot connect to Broker - " + broker);
            jmse.printStackTrace();
            System.exit(1);
        }

        // Create the Queue and QueueRequestor for sending requests.
        javax.jms.Queue queue = null;
        try
        {
            queue = session.createQueue (sQueue);
            requestor = new javax.jms.QueueRequestor(session, queue);

            // Now that all setup is complete, start the Connection.
            connect.start();
        }
        catch (javax.jms.JMSException jmse)
        {
            jmse.printStackTrace();
            exit();
        }

        try
        {
            // Read all standard input and send it as a message.
            java.io.BufferedReader stdin =
                new java.io.BufferedReader( new java.io.InputStreamReader( System.in ) );
            System.out.println ("\nRequestor application:\n"
			            					+ "============================\n"
			            					+ "The application user " + username + " connects to the broker at " + DEFAULT_BROKER_NAME + ".\n"
											+ "The application uses a QueueRequestor to on the " + DEFAULT_QUEUE + " queue."
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
                    // Instead of sending, we will use the QueueRequestor.
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
            requestor.close();
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
        String queue     = DEFAULT_QUEUE;

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

            if (arg.equals("-qs")) {
                if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                    System.err.println("error: missing queue");
                    System.exit(1);
                }
                queue = argv[++i];
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

        // Start the JMS client for sending requests.
        Requestor requestor = new Requestor();
        requestor.start (broker, username, password, queue);

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
        use.append("  -qs queue    Specify name of queue for sending.\n");
        use.append("               Default queue: "+DEFAULT_QUEUE+"\n");
        use.append("  -h           This help screen.\n");
        System.err.println (use);
    }

}

