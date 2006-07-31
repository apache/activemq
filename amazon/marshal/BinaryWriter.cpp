/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#include "BinaryWriter.h"

#include <netinet/in.h>

using namespace ActiveMQ::IO;

void BinaryWriter::writeByte(uint8_t value)
{
    // Write a single byte
    write((uint8_t *)&value, sizeof(char));
}

void BinaryWriter::writeBoolean(bool value)
{
    // Write a boolean
    write((uint8_t *)&value, sizeof(bool));
}

void BinaryWriter::writeDouble(double value)
{
    write((uint8_t *)&value, sizeof(double));
}

void BinaryWriter::writeFloat(float value)
{
    write((uint8_t *)&value, sizeof(float));
}

void BinaryWriter::writeShort(short value)
{
    // Write a short, convert from little endian to big endian if necessary
    short v = htons(value);
    write((uint8_t *)&v, sizeof(short));
}

void BinaryWriter::writeInt(int value)
{
    // Write an int, convert from little endian to big endian if necessary
    int v = htonl(value);
    write((uint8_t *)&v, sizeof(int));
}

void BinaryWriter::writeLong(int64_t value)
{
    // Write a int64_t, convert from little endian to big endian if necessary
    //int64_t v = htonl(value);
    write((uint8_t *)&value, sizeof(int64_t));
}

void BinaryWriter::writeString(const std::string& value)
{
    // Write length of string
    short length = (short)value.length();
    writeShort( length );

    // Write string contents
    write((uint8_t*)value.c_str(), length);
}
