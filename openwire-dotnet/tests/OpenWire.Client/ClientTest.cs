using System;
using System.IO;

using NUnit.Framework;

using OpenWire.Client;
using OpenWire.Client.Core;

namespace OpenWire.Client
{
    
    [ TestFixture ]
    public class ClientTest : TestSupport
    {
        
        [ Test ]
        public void CreateOpenWireFormat()
        {
            OpenWireFormat format = new OpenWireFormat();
            Assert.IsTrue(format != null);
        }
        
        [ Test ]
        public void CreateConnectionFactory()
        {
            IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
            Assert.IsTrue(factory != null, "created valid factory: " + factory);
        }
        
        [ Test ]
        public void SendAndSyncReceive()
        {
            IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
            
            Assert.IsTrue(factory != null, "no factory created");
            
            Console.WriteLine("Worked!");
            
            using (IConnection connection = factory.CreateConnection())
            {
                try
                {
                    Assert.IsTrue(connection != null, "no connection created");
                    Console.WriteLine("Created a connection!");
                    
                    ISession session = connection.CreateSession();
                    Console.WriteLine("Created a session: " + session);
                    
                    IDestination destination = session.GetQueue("FOO.BAR");
                    Assert.IsTrue(destination != null, "No queue available!");
                    Console.WriteLine("Using destination: " + destination);
                    
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    Console.WriteLine("Created consumer!: " + consumer);
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    Console.WriteLine("Created producer!: " + producer);
                    
                    string expected = "Hello World!";
                    ITextMessage request = session.CreateTextMessage(expected);
                    Console.WriteLine("### About to send message: " + request);
                    
                    producer.Send(request);
                    Console.WriteLine("### Sent message!");
                    
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
                catch (Exception e)
                {
                    Console.WriteLine("Caught: " + e);
                }
            }
        }
    }
}

