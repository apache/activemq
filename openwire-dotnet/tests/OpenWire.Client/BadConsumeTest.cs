using NUnit.Framework;
using System;

namespace OpenWire.Client
{
    [TestFixture]
    public class BadConsumeTest : TestSupport
    {
        [Test]
        public void TestBadConsumeOperationToTestExceptions()
        {
            IConnectionFactory factory = new ConnectionFactory("localhost", 61616);
            using (IConnection connection = factory.CreateConnection())
            {
                ISession session = connection.CreateSession();
                
                try
                {
                    IMessageConsumer consumer = session.CreateConsumer(null);
                    Assert.Fail("Should  have thrown an exception!");
                }
                catch (BrokerException e)
                {
                    Console.WriteLine("Caught expected exception: " + e);
                    Console.WriteLine("Stack: " + e.StackTrace);
                    Console.WriteLine("BrokerStrack: " + e.BrokerError.StackTrace);
                }
            }
        }
    }
}
