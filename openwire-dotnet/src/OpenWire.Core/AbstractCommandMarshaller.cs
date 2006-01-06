using System;

namespace OpenWire.Core
{
	/// <summary>
	/// A base class with useful implementation inheritence methods 
	/// for creating marshallers of the OpenWire protocol
	/// </summary>
	public abstract class AbstractCommandMarshaller {

        public abstract Command CreateCommand();

        public abstract void BuildCommand(Command command, BinaryReader dataIn);
        
        public abstract void WriteCommand(Command command, BinaryWriter dataOut);
        
	}
}

