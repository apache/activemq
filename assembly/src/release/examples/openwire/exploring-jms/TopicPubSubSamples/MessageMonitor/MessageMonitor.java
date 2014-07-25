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

Writing a Basic JMS Application Using
    Subscribe

When you run this program, it will
subscribe to any group of messages specified
in a properties file. [See comments in
MessageMonitor.properties for information on
this file.]

Every message topic being monitored will be
displayed to a Java window.

Usage:
  java MessageMonitor

Suggested demonstration:
  - Start one instance of this application:
        java MessageMonitor
  - Run one or more Chat and/or DurableChat window(s).
  - Enter messages on the various chat windows.
  - Watch the MessageMonitor display the messages.
*/
import org.apache.activemq.*;

import javax.swing.JTextArea;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JButton;

import javax.swing.text.Highlighter;
import javax.swing.text.DefaultHighlighter;
import javax.swing.text.BadLocationException;

import javax.swing.border.CompoundBorder;
import javax.swing.border.EmptyBorder;
import javax.swing.border.BevelBorder;
import javax.swing.border.SoftBevelBorder;

import java.awt.Toolkit;
import java.awt.Dimension;
import java.awt.BorderLayout;
import java.awt.Rectangle;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;

import javax.jms.Topic;
import javax.jms.Session;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;


import java.io.FileInputStream;

import java.util.Vector;
import java.util.Properties;
import java.util.StringTokenizer;

public class MessageMonitor
extends JFrame
{
    private static final String DEFAULT_PROPERTIES_FILE = "MessageMonitor.properties";

    String propertiesFile = DEFAULT_PROPERTIES_FILE;
    String brokerHostPort = "localhost";
    String connectID = "MessageMonitor";
    String userID = "Administrator";
    
    String subscriptionTopics = "jms.samples.chat";
    String textFontName = "Dialog";
    String textFontStyle = "PLAIN";
    String textFontSize = "12";
    String title = "MessageMonitor";

    JTextArea textArea = new JTextArea();
    JScrollPane scrollPane = new JScrollPane(textArea);
    JButton clearButton = new JButton("Clear");

    Connection connection = null;
    Session session = null;

    private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = ActiveMQConnection.DEFAULT_BROKER_URL;


    /** Constructor for MessageMonitor window. */
    public MessageMonitor()
    {
        loadProperties();

        setTitle(title);

        // Connect to Message Broker
        try
        {
            javax.jms.ConnectionFactory factory;
            factory = new ActiveMQConnectionFactory(user, password, url);

            connection = factory.createConnection (userID, password);
            session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("Cannot connect to Broker");
            jmse.printStackTrace();
            System.exit(1);
        }

        // Subscribe to Topics
        StringTokenizer topics = new StringTokenizer(subscriptionTopics, ",");
        while (topics.hasMoreTokens())
        {
            try
            {
                String topicName = topics.nextToken();
                System.out.println ("Subscribing to: " + topicName);
                new Subscription(session.createTopic(topicName));
            }
            catch (javax.jms.JMSException jmse)
            {
                jmse.printStackTrace();
            }
        }

        // Set up the viewing area.
        textArea.setEditable(false);
        scrollPane.setBorder(new CompoundBorder(new EmptyBorder(6,6,6,6),
                                                new SoftBevelBorder(BevelBorder.LOWERED)));
        getContentPane().add(scrollPane,BorderLayout.CENTER);
        getContentPane().add(clearButton,BorderLayout.SOUTH);
        clearButton.addActionListener(new OnClear());
        // Start the connection so that we can now receive messages.
        try
        {
            connection.start();
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("Cannot start connection");
            jmse.printStackTrace();
            System.exit(1);
        }

    }

    /** Class to handle the subsciption to messages. */
    public class Subscription implements javax.jms.MessageListener
    {
        public Subscription(Topic topic)
        {
            try
            {
                topicSubscriber = session.createConsumer(topic);
                topicSubscriber.setMessageListener(this);
            }
            catch (javax.jms.JMSException jmse)
            {
                jmse.printStackTrace();
            }
        }

       /**
        * Handle the text message
        * (as specified in the javax.jms.MessageListener interface).
        */

        public void onMessage(javax.jms.Message message)
        {
            String msgBody = null;
            String msgClass = null;

            if (message instanceof javax.jms.TextMessage)
            {
                msgClass = "javax.jms.TextMessage";
                try
                {
                    msgBody = ((javax.jms.TextMessage)message).getText();
                }
                catch (javax.jms.JMSException jmse)
                {
                    msgBody = "";
                }
            }
            else
            {
                return;
            }
            try
            {
                textArea.append("\n");
                textArea.append("-----------------------------------------------------------------------------------------------------\n");
                // textArea.append("Class: " + msgClass + "\n");
                textArea.append("The following message, received on topic " + ((Topic)message.getJMSDestination()).getTopicName() + ", was sent by\n");
                //textArea.append("\n");
                textArea.append(msgBody);

                // Ensure Appended Text is Visible
                Rectangle area = textArea.modelToView(textArea.getText().length());
                if (area != null) textArea.scrollRectToVisible(area);

            }
            catch (javax.jms.JMSException jmse)
            {
                jmse.printStackTrace();
            }
            catch (BadLocationException ble)
            {
                ble.printStackTrace();
            }

        }

        MessageConsumer topicSubscriber = null;
    }

    //
    // NOTE: the remainder of this sample deals with reading arguments
    // and does not utilize any JMS classes or code.
    //

    /** Main program entry point. */
    public static void main(String[] args)
    {
        // There should be no arguments to this program.
        if (args.length > 0) {
            printUsage();
            System.exit(1);
        }

        MessageMonitor messageMonitor = new MessageMonitor();

        messageMonitor.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e)
            {
                System.exit(0);
            }
        });

        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        screenSize.height = screenSize.height / 2 ;
        screenSize.width = screenSize.width / 2 ;
        messageMonitor.setSize(screenSize);
        messageMonitor.setVisible(true);

    }

    /** Prints the usage. */
    private static void printUsage()
    {
        StringBuffer use = new StringBuffer();
        use.append("\nusage: MessageMonitor\n\n");
        use.append("Properties for this sample can be set in a properties file.\n");
        String dfltFile = System.getProperty("propertiesFile", DEFAULT_PROPERTIES_FILE);
        use.append("[Default file: " + dfltFile +"]\n\n");

        System.out.print(use);
    }

    /** Load the window and JMS properties from a file. */
    private void loadProperties()
    {
        try
        {
            Properties properties = new Properties();

            propertiesFile = System.getProperty("propertiesFile", propertiesFile);

            properties.load(new FileInputStream(propertiesFile));

            // Connection Properties
            brokerHostPort = properties.getProperty("brokerHostPort",brokerHostPort).trim();
            connectID = properties.getProperty("connectID",connectID).trim();
            userID = properties.getProperty("userID",userID).trim();
            password = properties.getProperty("password",password).trim();

            // Subscription Properties
            subscriptionTopics = properties.getProperty("subscriptionTopics", subscriptionTopics).trim();

            // Text Properties
            textFontName = properties.getProperty("textFontName", textFontName).trim();
            textFontStyle = properties.getProperty("textFontSize", textFontStyle).trim();
            textFontSize = properties.getProperty("textFontSize", textFontSize).trim();

            // Window Properties
            title = properties.getProperty("title", title).trim();

        }
        catch (java.io.FileNotFoundException fnfe)
        {
            System.out.println (propertiesFile + " not found: using defaults"); // Use Defaults
        }
        catch (java.io.IOException ioe)
        {
            ioe.printStackTrace();
        }
    }

    /** Class to handle the "Clear" button action. */
    public class OnClear implements ActionListener
    {
        public void actionPerformed(ActionEvent evt)
        {
            textArea.setText("");
        }
    }

}
