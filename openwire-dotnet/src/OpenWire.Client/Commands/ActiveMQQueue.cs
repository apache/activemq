using System;
using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    /// <summary>
    /// Summary description for ActiveMQQueue.
    /// </summary>
    public class ActiveMQQueue : ActiveMQDestination, IQueue
    {
        public const byte ID_ActiveMQQueue = 100;
        
        public ActiveMQQueue() : base()
        {
        }
        public ActiveMQQueue(String name) : base(name)
        {
        }
        
        public String QueueName
        {
            get { return PhysicalName; }
        }
        
        public override byte GetDataStructureType()
        {
            return ID_ActiveMQQueue;
        }
        
        public override int GetDestinationType()
        {
            return ACTIVEMQ_QUEUE;
        }
        
        public override ActiveMQDestination CreateDestination(String name)
        {
            return new ActiveMQQueue(name);
        }
    }
}
