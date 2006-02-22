using System;
using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Core {
        /// <summary>
        /// Summary description for DataStructureSupport.
        /// </summary>
        public abstract class DataStructureSupport : DataStructure {

                protected DataStructureSupport() {
                }

                public virtual byte GetDataStructureType() {
                        return 0; 
                }             
                
                public virtual bool IsMarshallAware() {
                        return false; 
                }     
        }
}
