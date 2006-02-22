using System;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core {
        /// <summary>
        /// An OpenWire command
        /// </summary>
        public interface DataStructure {

                byte GetDataStructureType();
                bool IsMarshallAware();
        }
}
