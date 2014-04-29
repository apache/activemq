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

Queue/PTP
Send and receive using multiple sessions and a temporary queue

This sample shows the round trip time for a message being sent to the broker
and received. A temporary queue is used to guarantee that each instance of this
sample receives its own messages only.

Usage:

    java QueueRoundTrip -b <broker:port> -u <username> -p <password> -n <numTests> -h
        Optional Parameters:
        -b  broker:port    Broker name and port of your message server
                            Default: tcp://localhost:61616
        -u  username       Default: user (username required)
        -p  password       Default: password (not checked)
        -n  numTests       The number of messages to be sent/received
                           Default: 100
        -h                 Prints help screen.
 */
import org.apache.activemq.*;


public class QueueRoundTrip
{
    private static final String DEFAULT_BROKER_NAME = "tcp://localhost:61616";
    private static final String DEFAULT_PASSWORD    = "password";
    private static final String DEFAULT_USER_NAME   = "user";
    private static final int DEFAULT_NUM_TESTS      = 100;

    private static final int msgSize = 1400;
    private static byte[] msgBody = new byte[msgSize];

    private javax.jms.ConnectionFactory factory = null;
    private javax.jms.Connection connection     = null;
    private javax.jms.Session sendSession       = null;
    private javax.jms.Session receiveSession    = null;
    private javax.jms.MessageProducer sender    = null;
    private javax.jms.MessageConsumer receiver  = null;

    private void QueueRoundTripper(String broker, String username, String password, int numTests){

        try
        {
            //Set up two sessions, one for sending and the other for receiving
            factory = new ActiveMQConnectionFactory(username, password, broker);
            connection = factory.createConnection(username, password);
            sendSession = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
            receiveSession = connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println ("error: Cannot connect to broker- " + broker + ".\n");
            jmse.printStackTrace ();
            System.exit(1);
        }

        try
        {
            //Set up a temporary Queue:
            javax.jms.TemporaryQueue tempQueue = sendSession.createTemporaryQueue();
            receiver = receiveSession.createConsumer(tempQueue);
            sender = sendSession.createProducer(tempQueue);
            connection.start();
        }
        catch (javax.jms.JMSException jmse){
            System.err.println("error: Connection couldn't be started.\n");
            jmse.printStackTrace();
            System.exit(1);
        }

        //Send messages using Temporary Queue:
        try {
            System.out.println("QueueRoundTrip application:");
            System.out.println("===========================");
            System.out.println("Sending Messages to Temporary Queue...");

            //create a message to send
            javax.jms.BytesMessage msg = sendSession.createBytesMessage();
            msg.writeBytes(msgBody);

            //send and receive the message the specified number of times:
            long time = System.currentTimeMillis();
            for (int i = 0; i < numTests; i++){
                sender.send(msg);
                msg = (javax.jms.BytesMessage)receiver.receive();
            }
            time = System.currentTimeMillis()-time;

            System.out.println("\nTime for " + numTests + " sends and receives:\t\t" +
                                time + "ms\n" +
                                "Average Time per message:\t\t\t" + (float)time/(float)numTests + "ms\n");
            System.out.println("\n\nPress Enter to close this window.");
            java.io.BufferedReader stdin =
                new java.io.BufferedReader( new java.io.InputStreamReader( System.in ) );
            stdin.readLine();
            System.exit(0);
        }

        catch (javax.jms.JMSException jmse) {
            System.err.println("error: message not sent/received.\n");
            jmse.printStackTrace();
            System.exit(1);
        }

        catch (java.io.IOException ioe) {
            ioe.printStackTrace();
        }


    }

        /** Cleanup resources and then exit. */
    private void exit()
    {
        try
        {
            connection.close();
        }
        catch (javax.jms.JMSException jmse)
        {
            jmse.printStackTrace();
        }

        System.exit(0);
    }

    public static void main (String argv[])
    {
        // Values to be read from parameters
        String broker    = DEFAULT_BROKER_NAME;
        String username  = DEFAULT_USER_NAME;
        String password  = DEFAULT_PASSWORD;
        int numTests = DEFAULT_NUM_TESTS;

        // Check parameters
        if(argv.length > 0){
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
                    if (arg.equals("-n")) {
                        if (i == argv.length - 1 || argv[i+1].startsWith("-")) {
                            System.err.println("error: missing number of test to run.");
                            System.exit(1);
                        }
                        numTests = (new Integer(argv[++i])).intValue();
                        continue;
                    }

                    if (arg.equals("-h")) {
                        printUsage();
                        System.exit(1);
                    }
                }
            }
        }

        // create the payload
        byte charToWrite = (0x30);
        for (int i = 0; i < msgSize; i++)
        {
            msgBody[i] = charToWrite;
            charToWrite = (byte) ((int) charToWrite + (int) 0x01);
            if (charToWrite == (0x39))
            {
                charToWrite = (0x30);
            }
        }

        // Start the JMS client for the test.
        QueueRoundTrip queueRoundTripper = new QueueRoundTrip();
        queueRoundTripper.QueueRoundTripper(broker, username, password, numTests);

    }

    private static void printUsage()
    {
        StringBuffer use = new StringBuffer();

        use.append("Usage:\n");
        use.append("java QueueRoundTrip (options)...\n\n");
        use.append("options:\n");
        use.append("-b  broker:port    Broker name and port of your message server\n");
        use.append("                   Default: tcp://localhost:61616\n");
        use.append("-u  username       Default: user (username required)\n");
        use.append("-p  password       Default: password (not checked)\n");
        use.append("-n  numTests       The number of messages to be sent/received\n");
        use.append("                   Default: 100\n");
        use.append("-h                 This help screen");
        System.err.println (use);
    }

}
