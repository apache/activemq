using System;
using OpenWire.Client.Commands;

namespace OpenWire.Client
{
    public delegate void MessageHandler(IMessage message);
    
    /// <summary>
    /// A consumer of messages
    /// </summary>
    public interface IMessageConsumer : IDisposable
    {
        
		/// <summary>
		/// Waits until a message is available and returns it
		/// </summary>
		IMessage Receive();
		
		/// <summary>
        /// If a message is available within the timeout duration it is returned otherwise this method returns null
        /// </summary>
        IMessage Receive(long timeout);
        
        /// <summary>
        /// If a message is available immediately it is returned otherwise this method returns null
        /// </summary>
        IMessage ReceiveNoWait();
        
        /// <summary>
        /// An asynchronous listener which can be used to consume messages asynchronously
        /// </summary>
        event MessageHandler Listener;
        
    }
}
