/*
* Copyright 2006 The Apache Software Foundation or its licensors, as
* applicable.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using ActiveMQ.OpenWire;
using NMS;

namespace ActiveMQ.Commands
{
    //
    //  Marshalling code for Open Wire Format for WireFormatInfo
    //
    //
    public class WireFormatInfo : AbstractCommand, Command, MarshallAware
    {
        public const byte ID_WireFormatInfo = 1;
		static private byte[] MAGIC = new byte[] {
			'A'&0xFF,
			'c'&0xFF,
			't'&0xFF,
			'i'&0xFF,
			'v'&0xFF,
			'e'&0xFF,
			'M'&0xFF,
			'Q'&0xFF };
		
        byte[] magic = MAGIC;
        int version;
        byte[] marshalledProperties;
		
        protected static MessagePropertyHelper propertyHelper = new MessagePropertyHelper();
        private PrimitiveMap properties;
		
		public override string ToString() {
            return GetType().Name + "["
                + " Magic=" + Magic
                + " Version=" + Version
                + " MarshalledProperties=" + MarshalledProperties
                + " ]";

		}
	
        public override byte GetDataStructureType() {
            return ID_WireFormatInfo;
        }


        // Properties
        public byte[] Magic
        {
            get { return magic; }
            set { this.magic = value; }
        }

        public int Version
        {
            get { return version; }
            set { this.version = value; }
        }

        public byte[] MarshalledProperties
        {
            get { return marshalledProperties; }
            set { this.marshalledProperties = value; }
        }
		
		public IPrimitiveMap Properties
        {
            get {
                if (properties == null)
                {
                    properties = PrimitiveMap.Unmarshal(MarshalledProperties);
                }
                return properties;
            }
        }

        public bool StackTraceEnabled
        {
            get { return true.Equals(Properties["StackTraceEnabled"]) ; }
            set { Properties["StackTraceEnabled"] = value; }
        }
        public bool TcpNoDelayEnabled
        {
            get { return true.Equals(Properties["TcpNoDelayEnabled"]); }
            set { Properties["TcpNoDelayEnabled"] = value; }
        }
        public bool SizePrefixDisabled
        {
            get { return true.Equals(Properties["SizePrefixDisabled"]); }
            set { Properties["SizePrefixDisabled"] = value; }
        }
        public bool TightEncodingEnabled
        {
            get { return true.Equals(Properties["TightEncodingEnabled"]); }
            set { Properties["TightEncodingEnabled"] = value; }
        }
        public bool CacheEnabled
        {
            get { return true.Equals(Properties["CacheEnabled"]); }
            set { Properties["CacheEnabled"] = value; }
        }
		
		// MarshallAware interface
        public override bool IsMarshallAware()
        {
            return true;
        }
        
        public override void BeforeMarshall(OpenWireFormat wireFormat)
        {
            MarshalledProperties = null;
            if (properties != null)
            {
                MarshalledProperties = properties.Marshal();
            }
        }
		

    }
}
