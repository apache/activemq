using System;

using OpenWire.Core;
using OpenWire.Core.Commands;

namespace OpenWire.Core.Transport {
        
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
