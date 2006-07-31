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
#include "marshal/BinaryReader.h"

#include <netinet/in.h>

using namespace ActiveMQ::IO;
using namespace std;

uint8_t BinaryReader::readByte()
{
    uint8_t value;

    // Read a single byte
    read(&value, sizeof(uint8_t));
    return value;
}

bool BinaryReader::readBoolean()
{
    bool value;

    // Read a boolean
    read((uint8_t*)&value, sizeof(bool));
    return value;
}

double BinaryReader::readDouble()
{
    double value;

    read((uint8_t*)&value, sizeof(double) );
    return value;
}

float BinaryReader::readFloat()
{
    float value;

    read((uint8_t*)&value, sizeof(float));
    return value;
}

short BinaryReader::readShort()
{
    short value;

    // Read a short and convert from big endian to little endian if necessary
    read((uint8_t*)&value, sizeof(short));
    return ntohs(value);
}

int BinaryReader::readInt()
{
    int value;

    // Read an int and convert from big endian to little endian if necessary
    read((uint8_t*)&value, sizeof(int));
    return ntohl(value);
}

int64_t BinaryReader::readLong()
{
    int64_t value;

    // Read a int64_t and convert from big endian to little endian if necessary
    read((uint8_t*)&value, sizeof(int64_t));
    return value;
}

string BinaryReader::readString()
{
    string    value;
    char*     buffer;
    short     length;

    // Read length of string
    length = readShort();
    buffer = new char[ntohl(length)+1];

    // Read string bytes
    read((uint8_t *)buffer, length);
    *(buffer+length) = '\0';

    // Create string class
    value.assign(buffer);

    delete[] buffer;
    
    return value;
}
