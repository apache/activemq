/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System.IO;
using System;

namespace ActiveMQ.OpenWire
{
	/// <summary>
	/// Support class that switches from one endian to the other.
	/// </summary>
	[CLSCompliant(false)]
	public class EndianSupport
	{
		
        public static char SwitchEndian(char x)
        {
			return (char) (
				(((char)( (byte)(x)       )) << 8 ) |
				(((char)( (byte)(x >> 8)  )) )
				);
        }
        		
        public static short SwitchEndian(short x)
        {
			return (short) (
				(((ushort)( (byte)(x)       )) << 8 ) |
				(((ushort)( (byte)(x >> 8)  )) )
				);
        }
				
        public static int SwitchEndian(int x)
        {
			return
				(((int)( (byte)(x)       )) << 24 ) |
				(((int)( (byte)(x >> 8)  )) << 16 ) |
				(((int)( (byte)(x >> 16) )) << 8  ) |
				(((int)( (byte)(x >> 24) )) );
        }
		
        public static long SwitchEndian(long x)
        {
			return
				(((long)( (byte)(x     )  )) << 56 ) |
				(((long)( (byte)(x >> 8)  )) << 48 ) |
				(((long)( (byte)(x >> 16) )) << 40 ) |
				(((long)( (byte)(x >> 24) )) << 32 ) |
				(((long)( (byte)(x >> 32) )) << 24 ) |
				(((long)( (byte)(x >> 40) )) << 16 ) |
				(((long)( (byte)(x >> 48) )) << 8  ) |
				(((long)( (byte)(x >> 56) )) );
        }
		
        public static ushort SwitchEndian(ushort x)
        {
			return (ushort) (
				(((ushort)( (byte)(x)       )) << 8 ) |
				(((ushort)( (byte)(x >> 8)  )) )
				);
        }
		
        public static uint SwitchEndian(uint x)
        {
			return
				(((uint)( (byte)(x     )  )) << 24 ) |
				(((uint)( (byte)(x >> 8)  )) << 16 ) |
				(((uint)( (byte)(x >> 16) )) << 8  ) |
				(((uint)( (byte)(x >> 24) )) );
        }
        
        public static ulong SwitchEndian(ulong x)
        {
			return
				(((ulong)( (byte)(x     )  )) << 56 ) |
				(((ulong)( (byte)(x >> 8)  )) << 48 ) |
				(((ulong)( (byte)(x >> 16) )) << 40 ) |
				(((ulong)( (byte)(x >> 24) )) << 32 ) |
				(((ulong)( (byte)(x >> 32) )) << 24 ) |
				(((ulong)( (byte)(x >> 40) )) << 16 ) |
				(((ulong)( (byte)(x >> 48) )) << 8  ) |
				(((ulong)( (byte)(x >> 56) )) );
        }
		
	}
}

