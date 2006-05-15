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
#include "ppr/io/encoding/CharsetEncoderRegistry.hpp"
#include <cctype>
#include <algorithm>

using namespace apache::ppr::io::encoding;

// Init static members
map<string, p<ICharsetEncoder> > CharsetEncoderRegistry::encoders ;
const char* CharsetEncoderRegistry::DEFAULT = AsciiToUTF8Encoder::NAME ;

// Init the default set of encoders
static CharsetEncoderRegistry::MapInitializer initializer ;


// --- Constructors -------------------------------------------------

/*
 *
 */
CharsetEncoderRegistry::CharsetEncoderRegistry()
{
    // no-op
}

/*
 *
 */
CharsetEncoderRegistry::~CharsetEncoderRegistry()
{
    // no-op
}


// --- Attribute methods --------------------------------------------

/*
 *
 */
p<ICharsetEncoder> CharsetEncoderRegistry::getEncoder()
{
    return getEncoder( CharsetEncoderRegistry::DEFAULT ) ;
}

/*
 *
 */
p<ICharsetEncoder> CharsetEncoderRegistry::getEncoder(const char* name)
{
    // Assert argument
    if( name == NULL )
        return NULL ;

    map<string, p<ICharsetEncoder> >::iterator tempIter ;
    string key = string(name) ;

    // Make key string all lower case
    std::transform(key.begin(), key.end(), key.begin(), (int(*)(int))tolower) ;  // The explicit cast is needed to compile on Linux

    // Check if key exists in map
    tempIter = encoders.find( key ) ;
    if( tempIter != encoders.end() )
        return tempIter->second ;
    else   // Not found
        return NULL ;
}


// --- Operation methods --------------------------------------------

/*
 *
 */
void CharsetEncoderRegistry::addEncoder(const char* name, p<ICharsetEncoder> encoder) throw(IllegalArgumentException)
{
    // Assert arguments
    if( name == NULL || encoder == NULL )
        throw IllegalArgumentException("Name and/or encoder cannot be NULL") ;

    // Make key string all lower case
    string key = string(name) ;
    std::transform(key.begin(), key.end(), key.begin(), (int(*)(int))tolower) ;  // The explicit cast is needed to compile on Linux

    encoders[key] = encoder ;
}

/*
 *
 */
void CharsetEncoderRegistry::removeEncoder(const char* name) throw(IllegalArgumentException)
{
    // Assert argument
    if( name == NULL )
        throw IllegalArgumentException("Name cannot be NULL") ;

    map<string, p<ICharsetEncoder> >::iterator tempIter ;
    string key = string(name) ;

    // Make key string all lower case
    std::transform(key.begin(), key.end(), key.begin(), (int(*)(int))tolower) ;  // The explicit cast is needed to compile on Linux

    // Check if key exists in map
    tempIter = encoders.find( key ) ;
    if( tempIter != encoders.end() )
        encoders.erase( tempIter ) ;
}
