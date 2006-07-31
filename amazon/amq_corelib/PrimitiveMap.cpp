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

#include <utility>

#include "PrimitiveMap.h"
#include "netinet/in.h"

using namespace std;
using ActiveMQ::PrimitiveMap;
using ActiveMQ::Buffer;

static const unsigned char NULL_TYPE               = 0;
static const unsigned char BOOLEAN_TYPE            = 1;
static const unsigned char BYTE_TYPE               = 2;
static const unsigned char CHAR_TYPE               = 3;
static const unsigned char SHORT_TYPE              = 4;
static const unsigned char INTEGER_TYPE            = 5;
static const unsigned char LONG_TYPE               = 6;
static const unsigned char DOUBLE_TYPE             = 7;
static const unsigned char FLOAT_TYPE              = 8;
static const unsigned char STRING_TYPE             = 9;
static const unsigned char BYTE_ARRAY_TYPE         = 10;

PrimitiveMap::PrimitiveMap()
  {}

PrimitiveMap::PrimitiveMap(const Buffer& b) {
    // unmarshal from given buffer

    const unsigned char *buf = &(b[0]);

    int32_t count = ntohl(*((const int32_t*)buf));

    buf += 4;

    for (int i = 0; i < count; i++) {
        // get the size of the key string
        int16_t keysize = ntohs(*((const int16_t*)buf));
        buf += 2;
        // get the key string
        std::string key((const char *)(buf), keysize);
        buf += keysize;
        // get the type
        unsigned char type = *buf;
        ++buf;

        bool val;
        int bytearraylen;
        short strsize;
        
        switch (type) {
        case BOOLEAN_TYPE:
            val = (bool)(*buf);
            ++buf;
            booleanMap_.insert(pair<string, bool>(key, val));
            break;
            // ignore the other types for now
        case BYTE_TYPE:
            ++buf;
            break;
        case CHAR_TYPE:
            ++buf;
            break;
        case SHORT_TYPE:
            buf += 2;
            break;
        case INTEGER_TYPE:
            buf += 4;
            break;
        case LONG_TYPE:
            buf += 8;
            break;
        case FLOAT_TYPE:
            buf += 4;
            break;
        case DOUBLE_TYPE:
            buf += 8;
            break;
        case BYTE_ARRAY_TYPE:
            bytearraylen = ntohl(*(const int32_t*)buf);
            buf += 4;
            buf += bytearraylen;
            break;
        case STRING_TYPE:
            strsize = ntohs(*(const int16_t*)buf);
            buf += strsize;
            break;
        }
    }
}

void
PrimitiveMap::marshal(Buffer& b) const {
    // write the size
    int32_t size = htonl(booleanMap_.size());
    b.insert(b.end(), (unsigned char *)&size, ((unsigned char *)(&size)) + 4);

    // write each key/val pair
    for (map<string, bool>::const_iterator i = booleanMap_.begin();
         i != booleanMap_.end(); ++i) {
        // write the key size
        int16_t keysize = htons(i->first.size());
        b.insert(b.end(), (unsigned char *)&keysize, ((unsigned char *)(&keysize)) + 2);

        // write the key
        b.insert(b.end(), (unsigned char *)i->first.c_str(), ((unsigned char *)i->first.c_str()) + ntohs(keysize));

        // write the value type
        b.push_back(BOOLEAN_TYPE);

        // write the value
        b.push_back((unsigned char)i->second);
    }
}
