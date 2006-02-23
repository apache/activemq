using NUnit.Framework;
using OpenWire.Client.Core;
using System;

namespace openwire_dotnet
{
    [TestFixture]
    public class EndianTest
    {
        
        [Test]
        public void TestLongEndian()
        {
            long value = 0x0102030405060708l;
            
            long newValue = DataStreamMarshaller.SwitchEndian(value);
            
            Console.WriteLine("New value: " + newValue);
            
            Assert.AreEqual(0x0807060504030201L, newValue);
            
            long actual = DataStreamMarshaller.SwitchEndian(newValue);
            
            Assert.AreEqual(value, actual);
        }
        
        [Test]
        public void TestIntEndian()
        {
            int value = 0x12345678;
            
            int newValue = DataStreamMarshaller.SwitchEndian(value);
            
            Console.WriteLine("New value: " + newValue);
            
            Assert.AreEqual(0x78563412, newValue);
            
            int actual = DataStreamMarshaller.SwitchEndian(newValue);
            
            Assert.AreEqual(value, actual);
        }
        [Test]
        public void TestShortEndian()
        {
            short value = 0x1234;
            
            short newValue = DataStreamMarshaller.SwitchEndian(value);
            
            Console.WriteLine("New value: " + newValue);
            
            Assert.AreEqual(0x3412, newValue);
            
            short actual = DataStreamMarshaller.SwitchEndian(newValue);
            
            Assert.AreEqual(value, actual);
        }
    }
}


