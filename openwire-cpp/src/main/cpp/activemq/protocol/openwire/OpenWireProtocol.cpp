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
#include "activemq/protocol/openwire/OpenWireProtocol.hpp"
#include "activemq/protocol/openwire/OpenWireMarshaller.hpp"

using namespace apache::activemq::protocol::openwire;

// --- Static initialization ----------------------------------------

const char OpenWireProtocol::MAGIC[8]         = { 'A', 'c', 't', 'i', 'v', 'e', 'M', 'Q' } ;
const int  OpenWireProtocol::PROTOCOL_VERSION = 1 ;
const char OpenWireProtocol::NULL_TYPE        = 0 ;


/*
 * 
 */
OpenWireProtocol::OpenWireProtocol()
{
    array<char> magic (8);
    memcpy (magic.c_array(), "ActiveMQ", 8);

    // Create and configure wire format
    wireFormatInfo = new WireFormatInfo() ;
    wireFormatInfo->setMagic( magic ) ;
    wireFormatInfo->setVersion( PROTOCOL_VERSION ) ;
    wireFormatInfo->setStackTraceEnabled(true) ;
    wireFormatInfo->setTcpNoDelayEnabled(true) ;
    wireFormatInfo->setSizePrefixDisabled(false) ;
    wireFormatInfo->setTightEncodingEnabled(false) ;

    // Use variable instead of map lookup for performance reason
    this->sizePrefixDisabled = wireFormatInfo->getSizePrefixDisabled() ;

    // Create wire marshaller
    wireMarshaller = new OpenWireMarshaller(wireFormatInfo) ;
}

/*
 * 
 */
p<WireFormatInfo> OpenWireProtocol::getWireFormatInfo()
{
    return wireFormatInfo ;
}

/*
 * 
 */
bool OpenWireProtocol::getStackTraceEnabled()
{
    return wireFormatInfo->getStackTraceEnabled() ;
}

/*
 * 
 */
void OpenWireProtocol::handshake(p<ITransport> transport)
{
    // Send the wireformat we're using
    transport->oneway( getWireFormatInfo() ) ;
}

/*
 * 
 */
void OpenWireProtocol::marshal(p<IDataStructure> object, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;
    int                 size = 0 ;

    // Was a non-NULL object supplied
    if( object != NULL )
    {
        unsigned char dataType = object->getDataStructureType() ;

        // Calculate size to be marshalled if configured
        if( !sizePrefixDisabled )
        {
            size  = 1 ; // data structure type
            size += object->marshal(wireMarshaller, IMarshaller::MARSHAL_SIZE, ostream) ;

            // Write size header
            dos->writeInt(size) ;
        }
        // Finally, write command type and body
        dos->writeByte(dataType) ;
        object->marshal(wireMarshaller, IMarshaller::MARSHAL_WRITE, ostream) ;
    }
    else   // ...NULL object
    {
        // Calculate size to be marshalled if configured
        if( !sizePrefixDisabled )
        {
            // Calculate size to be marshalled
            size = 1 ; // data structure type

            // Write size header
            dos->writeInt(size) ;
        }
        // Write NULL command type and empty body
        dos->writeByte(NULL_TYPE) ;
    }
}

/*
 * 
 */
p<IDataStructure> OpenWireProtocol::unmarshal(p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;
    int                size = 0 ;

    // Read packet size if configured
    if( !sizePrefixDisabled )
        size = dis->readInt() ;

    // First byte is the data structure type
    unsigned char dataType = dis->readByte() ;

    // Check for NULL type
    if( dataType == NULL_TYPE )
        return NULL ;

    // Create command object
    p<IDataStructure> object = BaseDataStructure::createObject(dataType) ;
    if( object == NULL )
        throw IOException("Unmarshal failed; unknown data structure type %d, at %s line %d", dataType, __FILE__, __LINE__) ;

    // Finally, unmarshal command body
    object->unmarshal(wireMarshaller, IMarshaller::MARSHAL_READ, istream) ;
    return object ;
}

/*
 * 
 */
p<DataOutputStream> OpenWireProtocol::checkOutputStream(p<IOutputStream> ostream) throw (IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = p_dyncast<DataOutputStream> (ostream) ;
    if( dos == NULL )
        throw IOException("OpenWireProtocol requires a DataOutputStream") ;

    return dos ;
}

/*
 * 
 */
p<DataInputStream> OpenWireProtocol::checkInputStream(p<IInputStream> istream) throw (IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataInputStream> dis = p_dyncast<DataInputStream> (istream) ;
    if( dis == NULL )
        throw IOException("OpenWireProtocol requires a DataInputStream") ;

    return dis ;
}
