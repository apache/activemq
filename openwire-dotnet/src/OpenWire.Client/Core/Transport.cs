using System;

using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core {
        
        public delegate void CommandHandler(Transport sender, Command command);
        public delegate void ExceptionHandler(Transport sender, Exception command);

        /// <summary>
        /// Represents the logical networking transport layer.
        /// </summary>
        public interface Transport {
                void Oneway(Command command);

                FutureResponse AsyncRequest(Command command);

                Response Request(Command command);

                event CommandHandler Command;
                event ExceptionHandler Exception; 
        } 
}
