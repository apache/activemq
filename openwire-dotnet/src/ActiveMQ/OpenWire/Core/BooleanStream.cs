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
using System.IO;

using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// Represents a stream of boolean flags
    /// </summary>
    public class BooleanStream
    {
        byte[] data = new byte[48];
        short arrayLimit;
        short arrayPos;
        byte bytePos;
        
        public bool ReadBoolean()
        {
            byte b = data[arrayPos];
            bool rc = ((b >> bytePos) & 0x01) != 0;
            bytePos++;
            if (bytePos >= 8)
            {
                bytePos = 0;
                arrayPos++;
            }
            return rc;
        }
        
        public void WriteBoolean(bool value)
        {
            if (bytePos == 0)
            {
                arrayLimit++;
                if (arrayLimit >= data.Length)
                {
                    // re-grow the array.
                    byte[] d = new byte[data.Length * 2];
					Array.Copy(data, d, data.Length);
                    data = d;
                }
            }
            if (value)
            {
                data[arrayPos] |= (byte) (0x01 << bytePos);
            }
            bytePos++;
            if (bytePos >= 8)
            {
                bytePos = 0;
                arrayPos++;
            }
        }
        
        public void Marshal(BinaryWriter dataOut)
        {
			if( arrayLimit < 64 ) {
				dataOut.Write((byte)arrayLimit);
			} else if( arrayLimit < 256 ) { // max value of unsigned byte
				dataOut.Write((byte)0xC0);
				dataOut.Write((byte)arrayLimit);
			} else {
				dataOut.Write((byte)0x80);
				BaseDataStreamMarshaller.WriteShort(arrayLimit, dataOut);
			}
            dataOut.Write(data, 0, arrayLimit);
            Clear();
        }
        
        public void Unmarshal(BinaryReader dataIn)
        {
            arrayLimit = (short)(BaseDataStreamMarshaller.ReadByte(dataIn) & 0xFF);
			if ( arrayLimit == 0xC0 ) {
				arrayLimit = (short)(BaseDataStreamMarshaller.ReadByte(dataIn) & 0xFF);
			} else if( arrayLimit == 0x80 ) {
				arrayLimit = BaseDataStreamMarshaller.ReadShort(dataIn);
			}
			if( data.Length < arrayLimit ) {
				data = new byte[arrayLimit];
			}
			
            dataIn.Read(data, 0, arrayLimit);
            Clear();
        }
        
        public void Clear()
        {
            arrayPos = 0;
            bytePos = 0;
        }
        
        public int MarshalledSize()
        {
			if( arrayLimit < 64 ) {
				return 1+arrayLimit;
			} else if (arrayLimit < 256) {
				return 2+arrayLimit;
			} else {
				return 3+arrayLimit;
			}
			
        }
    }
}
