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
#include "ppr/util/Hex.hpp"

using namespace apache::ppr::util;

/*
 *
 */
Hex::Hex()
{
    // no-op
}

/*
 *
 */
Hex::~Hex()
{
    // no-op
}

/*
 * Converts a byte array into a hex string.
 */
p<string> Hex::toString(array<char> buffer)
{
    array<char> result ((buffer.size() * 2) + 1);
    p<string> hexStr ;

    // Format into a string
    for( int i = 0 ; i < (int)buffer.size() ; i++ )
        sprintf(&result[i*2], "%02x", (unsigned char) buffer[i]) ;

    hexStr = new string(result.c_array(), result.size() - 1) ;

    return hexStr ;
}
