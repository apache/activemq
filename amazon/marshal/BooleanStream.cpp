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

#include <math.h>

#include <bitset>
#include <iostream>

#include "marshal/BooleanStream.h"

using namespace ActiveMQ::IO;
using namespace std;

BooleanStream::BooleanStream() : pos_(0)
    {}

BooleanStream::BooleanStream(IReader& i) : pos_(0) {
    unsigned char sizemarker = i.readByte();
    short numbytes = 0;
    if (sizemarker == 0xC0) {
        numbytes = i.readByte();
    }
    else if (sizemarker == 0x80) {
        numbytes = i.readShort();
    }
    else {
        numbytes = sizemarker;
    }
    for (int j = 0; j < numbytes; j++) {
        unsigned char thisbyte = i.readByte();
        bitset<8> bs(thisbyte);
        for (int k = 0; k < 8; k++) {
            writeBoolean(bs[k]);
        }
    }
}

bool
BooleanStream::readBoolean() {
    return bools_[pos_++];
}

void
BooleanStream::writeBoolean(bool val) {
    bools_.push_back(val);
}

size_t
BooleanStream::getMarshalledSize() {
    size_t ret = 0;

    int size = bools_.size();
    short numtotalbytes = (short)(ceil((double)size / 8.0));
    unsigned char inchar = (unsigned char)numtotalbytes;
    if (numtotalbytes < 64)
        ++ret;
    else if (numtotalbytes < 256)
        ret += 2;
    else
        ret += 3;

    int numtowrite = size;
    int tmp = 0;
    while (numtowrite > 0) {
        bitset<8> byte;
        int i = 0;
        while (i < 8 && tmp != bools_.size())
            byte.set(i++,bools_[tmp++]);
        unsigned long tosend = byte.to_ulong();
        unsigned char thebyte = (unsigned char)tosend;
        ++ret;
        numtowrite -= i;
    }
    return ret;
}

void
BooleanStream::marshal(BinaryWriter& dataOut) {
    int size = bools_.size();
    short numtotalbytes = (short)(ceil((double)size / 8.0));
    unsigned char inchar = (unsigned char)numtotalbytes;
    if (numtotalbytes < 64) {
        dataOut.write(&inchar, 1);
    }
    else if (numtotalbytes < 256) {
        unsigned char tmp = 0xC0;
        dataOut.write(&tmp, 1);
        dataOut.write(&inchar, 1);
    }
    else {
        unsigned char tmp = 0x80;
        dataOut.write(&tmp, 1);
        dataOut.writeShort(numtotalbytes);
    }

    int numtowrite = size;
    int tmp = 0;
    while (numtowrite > 0) {
        bitset<8> byte;
        int i = 0;
        while (i < 8 && tmp != bools_.size())
            byte.set(i++,bools_[tmp++]);
        unsigned long tosend = byte.to_ulong();
        unsigned char thebyte = (unsigned char)tosend;
        dataOut.write(&thebyte, 1);
        numtowrite -= i;
    }
}
