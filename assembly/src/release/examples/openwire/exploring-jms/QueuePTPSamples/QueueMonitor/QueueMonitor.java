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
    - QueueBrowser
    - JMS with a Graphical Interface
    - behavior based on message type

When you run this program, it will read all the parameters out
of the QueueMonitor.properties file. In this file you can specify
which queues you want to monitor. Then a Java window will open and
every time you click the Browse button, The current contents of the queues
will be displayed in the text window.

Usage:
  java QueueMonitor

Suggested demonstration:
  - Start one instance of this application:
        java QueueMonitor
  - Run on or more Talk applications (without the receiving queue).
  - Enter messages on various Talk windows.
  - Watch the QueueMonitor display the messages.

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

import java.io.FileInputStream;

import java.util.Vector;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;

public class QueueMonitor
extends JFrame
{
    private static final String DEFAULT_PROPERTIES_FILE = "QueueMonitor.properties";

    String propertiesFile = DEFAULT_PROPERTIES_FILE;
    String broker = "tcp://localhost:61616";
    String connectID = "QueueMonitor";
    String username = "QueueMonitor";
    String password = "QueueMonitor";
    String browseQueues  = "Q1,Q2,Q3";
    String textFontName = "Dialog";
    String textFontStyle = "PLAIN";
    String textFontSize = "12";
    String title = "QueueMonitor";

    JTextArea textArea = new JTextArea();
    JScrollPane scrollPane = new JScrollPane(textArea);
    JButton browseButton = new JButton("Browse Queues");

    Vector theQueues = new Vector();

    private javax.jms.Connection connect = null;
    private javax.jms.Session session = null;

/** Constructor for MessageMonitor window. */
    public QueueMonitor()
    {
        loadProperties();

        setTitle(title);

        // Connect to Message Broker
        try
        {
            javax.jms.ConnectionFactory factory;
            factory = new ActiveMQConnectionFactory(username, password, broker);

            connect = factory.createConnection (username, password);
            session = connect.createSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("Cannot connect to Broker");
            jmse.printStackTrace();
            System.exit(1);
        }

        // Set up Queues:
        StringTokenizer queues = new StringTokenizer(browseQueues, ",");
        while (queues.hasMoreTokens())
        {
            try{
                String queueName = queues.nextToken();
                System.out.println ("Monitoring  " + queueName);
                theQueues.addElement(session.createQueue(queueName));
            }
            catch (javax.jms.JMSException jmse)
            {
                jmse.printStackTrace();
            }
        }


        // After init it is time to start the connection
        try
        {
            connect.start();
        }
        catch (javax.jms.JMSException jmse)
        {
            System.err.println("Cannot start connection");
            jmse.printStackTrace();
            System.exit(1);
        }

        //Elements visible on the screen
        textArea.setEditable(false);
        scrollPane.setBorder(new CompoundBorder(new EmptyBorder(6,6,6,6),
                                                new SoftBevelBorder(BevelBorder.LOWERED)));
        getContentPane().add(scrollPane,BorderLayout.CENTER);
        getContentPane().add(browseButton,BorderLayout.SOUTH);

        browseButton.addActionListener(new OnBrowse());

    }



    /** Main program entry point. */
    public static void main(String[] args)
    {
        // There should be no arguments to this program.
        if (args.length > 0) {
            printUsage();
            System.exit(1);
        }

        QueueMonitor queueMonitor = new QueueMonitor();

        queueMonitor.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e)
            {
                System.exit(0);
            }
        });

        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        screenSize.height = screenSize.height / 2 ;
        screenSize.width = screenSize.width / 2 ;
        queueMonitor.setSize(screenSize);
        queueMonitor.setVisible(true);

    }

    /** Prints the usage. */
    private static void printUsage()
    {
        StringBuffer use = new StringBuffer();
        use.append("\nusage: QueueMonitor\n\n");
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
            broker = properties.getProperty("broker",broker).trim();
            connectID = properties.getProperty("connectID",connectID).trim();
            username = properties.getProperty("username",username).trim();
            password = properties.getProperty("password",password).trim();

            // Queue Properties
            browseQueues = properties.getProperty("browseQueues", browseQueues).trim();

            // Text Properties
            textFontName = properties.getProperty("textFontName", textFontName).trim();
            textFontStyle = properties.getProperty("textFontStyle", textFontStyle).trim();
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

   /** Class to handle the "Browse" button action. */
    public class OnBrowse implements ActionListener
    {

        public void actionPerformed(ActionEvent evt)
        {
            // Clear the textArea.
            textArea.setText("");
            textArea.paintImmediately(textArea.getBounds());

            if(theQueues.size() == 0){
                textArea.setText("No Queues to be monitored");
            }
            else{
                for(int i = 0; i<theQueues.size();i++)
                {
                    try
                    {
                        // Create a browser on the queue and show the messages waiting in it.
                        javax.jms.Queue q = (javax.jms.Queue) theQueues.elementAt(i);
                       textArea.append("--------------------------------------------------\n");
                       textArea.append("Messages on queue " + q.getQueueName() + ":\n");
  
                        // Create a queue browser
                        System.out.print ("Browsing messages in queue " + q.getQueueName() + "\"...");
                        javax.jms.QueueBrowser browser = session.createBrowser(q);
                        System.out.println ("[done]");
                        int cnt = 0;
                        Enumeration e = browser.getEnumeration();
                        if(!e.hasMoreElements())
                        {
                            textArea.append ("(This queue is empty.)");
                        }
                        else
                        {
                            while(e.hasMoreElements())
                            {
                                System.out.print(" --> getting message " + String.valueOf(++cnt) + "...");
                                javax.jms.Message message = (javax.jms.Message) e.nextElement();
                                System.out.println("[" + message + "]");
                                if (message != null)
                                {
                                    String msgText = getContents (message);
                                    textArea.append(msgText + "\n");
                                    try
                                    {
                                        // Scroll the text area to show the message
                                        Rectangle area = textArea.modelToView(textArea.getText().length());
                                        textArea.scrollRectToVisible(area);
                                        textArea.paintImmediately(textArea.getBounds());
                                    }
                                    catch(Exception jle) { jle.printStackTrace();}
                                }
                            }
                        }
                        // Free any resources in the browser.
                        browser.close();
                        textArea.append ("\n");
                    }
                    catch (javax.jms.JMSException jmse){
                        jmse.printStackTrace();
                    }
                }
                try
                {
                    // Scroll the text area to show the message
                    Rectangle area = textArea.modelToView(textArea.getText().length());
                    textArea.scrollRectToVisible(area);
                    textArea.paintImmediately(textArea.getBounds());
                }
                catch(Exception jle) { jle.printStackTrace();}
            }
        }
    }

    public String getContents (javax.jms.Message message){


            String msgBody = null;
            String msgClass = message.getClass().getName();

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
          
            else if (message instanceof org.apache.activemq.command.ActiveMQMapMessage)
            {
		  			    System.out.println ("(Name value pairs in the MapMessage are not displayed.)");
            }
            else if (message instanceof javax.jms.BytesMessage)
          			{
		  			    System.out.println ("Warning: A bytes message was discarded because it could not be processed as a javax.jms.TextMessage.");
		  			 }
            else if (message instanceof javax.jms.ObjectMessage)
          			{
		  			    System.out.println ("Warning: An object message was discarded because it could not be processed as a javax.jms.TextMessage.");
		  			 }

            else if (message instanceof javax.jms.StreamMessage)
					{
			   			System.out.println ("Warning: A stream message was discarded because it could not be processed as a javax.jms.TextMessage.");
					 }
        return "- " + msgClass + " from " + msgBody ;

    }
}