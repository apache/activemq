using NUnit.Framework;
using OpenWire.Client.Commands;
using System.Collections;

namespace OpenWire.Client
{
    [TestFixture]
    public class CommandTest
    {
        
        [Test]
        public void TestCommand()
        {
            ConsumerId value1 = new ConsumerId();
            value1.ConnectionId = "abc";
            value1.SessionId = 123;
            value1.Value = 456;
            
            ConsumerId value2 = new ConsumerId();
            value2.ConnectionId = "abc";
            value2.SessionId = 123;
            value2.Value = 456;
            
            ConsumerId value3 = new ConsumerId();
            value3.ConnectionId = "abc";
            value3.SessionId = 123;
            value3.Value = 457;
            
            Assert.AreEqual(value1, value2, "value1 and value2 should be equal");
            Assert.AreEqual(value1.GetHashCode(), value2.GetHashCode(), "value1 and value2 hash codes should be equal");
            
            Assert.IsTrue(!value1.Equals(value3), "value1 and value3 should not be equal");
            Assert.IsTrue(!value3.Equals(value2), "value3 and value2 should not be equal");
            
            // now lets test an IDictionary
            IDictionary dictionary = new Hashtable();
            dictionary[value1] = value3;
            
            // now lets lookup with a copy
            object actual = dictionary[value2];
            
            Assert.AreEqual(value3, actual, "Should have found item in Map using value2 as a key");
        }
    }
}
