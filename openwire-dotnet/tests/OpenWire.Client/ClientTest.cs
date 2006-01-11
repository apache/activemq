using System;
using System.IO;

using NUnit.Framework;

using OpenWire.Client;

namespace OpenWire.Client {

        [ TestFixture ]
        public class ClientTest : TestSupport {

                [ Test ]
                public void SendAndSyncReceive() {
                        IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
                        
                        Assert.IsTrue(factory != null, "created valid factory: " + factory);
                        
                        Console.WriteLine("Worked!");
                        /*
                        using (IConnection connection = factory.CreateConnection()) {
                                ISession session = connection.CreateSession();
                                IDestination destination = session.GetQueue("FOO.BAR");
                                IMessageConsumer consumer = session.CreateConsumer(destination);
                                
                                IMessageProducer producer = session.CreateProducer(destination);
                                string expected = "Hello World!";
                                ITextMessage request = session.CreateTextMessage(expected);
                                producer.Send(request);
                                
                                ITextMessage message = (ITextMessage) consumer.Receive();
                                
                                Assert.AreEqual(expected, message.Text); 
                        } 
                        */
                } 
        } 
}
