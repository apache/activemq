using System;

using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core
{
    
    public delegate void CommandHandler(ITransport sender, Command command);
    public delegate void ExceptionHandler(ITransport sender, Exception command);
    
    /// <summary>
    /// Represents the logical networking transport layer.
    /// </summary>
    public interface ITransport : IStartable, IDisposable
    {
        void Oneway(Command command);
        
        FutureResponse AsyncRequest(Command command);
        
        Response Request(Command command);
        
        event CommandHandler Command;
        event ExceptionHandler Exception;
    }
}
