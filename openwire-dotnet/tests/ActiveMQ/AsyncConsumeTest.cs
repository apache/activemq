using System;
using System.IO;
using System.Threading;

using NUnit.Framework;

using OpenWire.Client;
using OpenWire.Client.Core;
namespace OpenWire.Client
{
    [TestFixture]
    public class AsyncConsumeTest : TestSupport
    {
        protected Object semaphore = new Object();
        protected bool received;
        
        [Test]
        public void TestAsynchronousConsume()
        {
            IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
            Assert.IsTrue(factory != null, "no factory created");
            
            using (IConnection connection = factory.CreateConnection())
            {
                Assert.IsTrue(connection != null, "no connection created");
                Console.WriteLine("Connected to ActiveMQ!");
                
                ISession session = connection.CreateSession();
                IDestination destination = CreateDestination(session);
                Assert.IsTrue(destination != null, "No queue available!");
                
                // lets create an async consumer
                // START SNIPPET: demo
                IMessageConsumer consumer = session.CreateConsumer(destination);
                consumer.Listener += new MessageListener(OnMessage);
                // END SNIPPET: demo
                
                
                // now lets send a message
                session = connection.CreateSession();
                IMessageProducer producer = session.CreateProducer(destination);
                IMessage request = CreateMessage(session);
                request.JMSCorrelationID = "abc";
                request.JMSType = "Test";
                producer.Send(request);
                
                
                WaitForMessageToArrive();
            }
            
        }
        
        protected void OnMessage(IMessage message)
        {
            Console.WriteLine("Received message: " + message);
            lock (semaphore)
            {
                received = true;
                Monitor.PulseAll(semaphore);
            }
            
        }
        
        protected void WaitForMessageToArrive()
        {
            lock (semaphore)
            {
                if (!received)
                {
                    Monitor.Wait(semaphore, 10000);
                }
            }
            Assert.AreEqual(true, received, "Should have received a message by now!");
        }
        
    }
}
