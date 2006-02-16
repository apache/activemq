using System;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core {
        /// <summary>
        /// An OpenWire command
        /// </summary>
        public interface Command : DataStructure {
                
                /* TODO
                short CommandId {
                        get;
                        set; 
                }

                bool ResponseRequired {
                        get;
                        set;
                } 
                */
        } 
}
