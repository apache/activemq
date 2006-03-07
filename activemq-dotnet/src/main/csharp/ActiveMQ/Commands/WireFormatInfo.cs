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

using System;
using System.Collections;

using ActiveMQ.OpenWire;
using ActiveMQ.Commands;
using JMS;

namespace ActiveMQ.Commands
{
    //
    //  Marshalling code for Open Wire Format for WireFormatInfo
    //
    //
    public class WireFormatInfo : AbstractCommand, Command, MarshallAware
    {
        public const byte ID_WireFormatInfo = 1;
    			
        byte[] magic;
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
            get { return true.Equals(Properties["stackTrace"]) ; }
            set { Properties["stackTrace"] = value; }
        }
        public bool TcpNoDelayEnabled
        {
            get { return true.Equals(Properties["tcpNoDelay"]); }
            set { Properties["tcpNoDelay"] = value; }
        }
        public bool PrefixPacketSize
        {
            get { return true.Equals(Properties["prefixPacketSize"]); }
            set { Properties["prefixPacketSize"] = value; }
        }
        public bool TightEncodingEnabled
        {
            get { return true.Equals(Properties["tightEncodingEnabled"]); }
            set { Properties["tightEncodingEnabled"] = value; }
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
