using System;
using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    /// <summary>
    /// Summary description for ActiveMQTempQueue.
    /// </summary>
    public class ActiveMQTempQueue : ActiveMQDestination, ITemporaryQueue
    {
        public const byte ID_ActiveMQTempQueue = 102;
        
        public ActiveMQTempQueue() : base()
        {
        }
        public ActiveMQTempQueue(String name) : base(name)
        {
        }
        
        public String GetQueueName()
        {
            return PhysicalName;
        }
        
        public override byte GetDataStructureType()
        {
            return ID_ActiveMQTempQueue;
        }
        
        public override int GetDestinationType()
        {
            return ACTIVEMQ_QUEUE;
        }
        
        public override ActiveMQDestination CreateDestination(String name)
        {
            return new ActiveMQTempQueue(name);
        }
    }
}
