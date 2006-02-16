package blah;

import java.util.Properties;

import javax.jms.*;
import javax.naming.*;

public class ActiveMQQueueSender
{
   public static void main(String[] args)
   {
      String msg = args.length < 1 ? "This is the default message" : args[0];

      Queue queue = null;
      QueueConnectionFactory queueConnectionFactory = null;
      QueueConnection queueConnection = null;

      try
      {
         Properties props = new Properties();
         //props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.evermind.server.rmi.RMIInitialContextFactory");
         //props.setProperty(Context.PROVIDER_URL, "ormi://10.1.0.99:3202/default");
         //props.setProperty(Context.SECURITY_PRINCIPAL, "dan");
         //props.setProperty(Context.SECURITY_CREDENTIALS, "abc123");
         
         props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.activemq.jndi.ActiveMQInitialContextFactory");
         props.setProperty(Context.PROVIDER_URL, "tcp://hostname:61616");
         props.setProperty("queue.BlahQueue", "example.BlahQueue");

         Context jndiContext = new InitialContext(props);
         
         //queueConnectionFactory = (QueueConnectionFactory) jndiContext.lookup("jms/QueueConnectionFactory");
         //queue = (Queue) jndiContext.lookup("jms/demoQueue");

         queueConnectionFactory = (QueueConnectionFactory) jndiContext.lookup("QueueConnectionFactory");
         queue = (Queue) jndiContext.lookup("BlahQueue");

      }
      catch (NamingException e)
      {
         System.out.println("---------------------------ERROR-----------------------------");
         e.printStackTrace();
         System.exit(-1);
      }

      try
      {
         queueConnection = queueConnectionFactory.createQueueConnection();
         QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         QueueSender queueSender = queueSession.createSender(queue);
         //queueSender.setDeliveryMode(DeliveryMode.PERSISTENT);
         //queueSender.setTimeToLive(1000*60*60);
         TextMessage message = queueSession.createTextMessage();

         message.setText(msg);
         message.setStringProperty("Blah", "Hello!");

         queueSender.send(message);
         System.out.println("Message sent");
      }
      catch (JMSException e)
      {
         System.out.println("SOMETHING WENT WRONG WHILE SENDING");
         e.printStackTrace();
      }
      finally
      {
         if (queueConnection != null)
         {
            try
            {
               queueConnection.close();
            }
            catch (Exception ignored)
            {
            }
         }
      }
   }
}
