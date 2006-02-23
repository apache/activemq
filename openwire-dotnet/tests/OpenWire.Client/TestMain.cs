using System;
using System.IO;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace openwire_dotnet
{
    public class TestMain
    {
        public static void Main(string[] args)
        {
            try
            {
                Console.WriteLine("About to connect to ActiveMQ");
                
                IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
                
                Console.WriteLine("Worked!");
                
                using (IConnection connection = factory.CreateConnection())
                {
                    Console.WriteLine("Created a connection!");
                    
                    ISession session = connection.CreateSession();
                    Console.WriteLine("Created a session: " + session);
                    
                    IDestination destination = session.GetQueue("FOO.BAR");
                    Console.WriteLine("Using destination: " + destination);
                    
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    string expected = "Hello World!";
                    ITextMessage request = session.CreateTextMessage(expected);
                    producer.Send(request);
                    
                    ITextMessage message = (ITextMessage) consumer.Receive();
                    if (message == null)
                    {
                        Console.WriteLine("### No message!!");
                    }
                    else
                    {
                        Console.WriteLine("### Received message: " + message + " of type: " + message.GetType());
                        String actual = message.Text;
                        
                        Console.WriteLine("### Message text is: " + actual);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Caught: " + e);
                Console.WriteLine("Stack: " + e.StackTrace);
            }
        }
    }
}
