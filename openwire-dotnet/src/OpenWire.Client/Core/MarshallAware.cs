using System;
using System.IO;

using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;

namespace OpenWire.Client.Core {
        /// <summary>
        /// Represents a marshallable entity
        /// </summary>
        public interface MarshallAware {

                void BeforeMarshall(OpenWireFormat wireFormat);
                void AfterMarshall(OpenWireFormat wireFormat);

                void BeforeUnmarshall(OpenWireFormat wireFormat);
                void AfterUnmarshall(OpenWireFormat wireFormat);

                void SetMarshalledForm(OpenWireFormat wireFormat, byte[] data);
                byte[] GetMarshalledForm(OpenWireFormat wireFormat);
        }
}
