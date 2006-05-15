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
#include "activemq/protocol/openwire/OpenWireMarshaller.hpp"

using namespace apache::activemq::protocol::openwire;

#ifdef MACOSX
#define BOOLSIZE 1
#else
#define BOOLSIZE sizeof(bool)
#endif


// --- Constructors -------------------------------------------------

/*
 * 
 */
OpenWireMarshaller::OpenWireMarshaller(p<WireFormatInfo> formatInfo)
{
    this->formatInfo = formatInfo ;
    this->encoder    = CharsetEncoderRegistry::getEncoder() ;
}

// --- Operation methods --------------------------------------------

/*
 * 
 */
int OpenWireMarshaller::marshalBoolean(bool value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
            dos->writeBoolean(value) ;

        return (int)BOOLSIZE ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalByte(char value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
            dos->writeByte(value) ;

        return (int)sizeof(char) ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalShort(short value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
            dos->writeShort(value) ;

        return (int)sizeof(short) ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalInt(int value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
            dos->writeInt(value) ;

        return (int)sizeof(int) ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalLong(long long value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
            dos->writeLong(value) ;

        return (int)sizeof(long long) ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalFloat(float value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
            dos->writeFloat(value) ;

        return (int)sizeof(float) ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalDouble(double value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
            dos->writeDouble(value) ;

        return (int)sizeof(double) ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalString(p<string> value, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( mode == IMarshaller::MARSHAL_WRITE )
        {
            dos->writeBoolean( value != NULL ) ; 
            dos->writeString(value) ;
        }
        int size = 0 ;

        // Null marker
        size += BOOLSIZE ;

        if( value != NULL )
        {
            // String char counter and length
            size += sizeof(short) ;
            size += ( encoder != NULL ) ? encoder->length(value) : (int)value->length() ;
        }
        return size ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalObject(p<IDataStructure> object, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        int size = 0 ;

        // Write data structure type
        if( mode == IMarshaller::MARSHAL_WRITE )
        {
            // Null marker
            dos->writeBoolean( object != NULL ) ;

            // Data structure type
            if( object != NULL )
                dos->writeByte( object->getDataStructureType() ) ;
        }

        // Length of null marker
        size += BOOLSIZE ;

        if( object != NULL )
        {
            // Length of data structure type
            size += sizeof(char) ;

            // Marshal the command body
            size += object->marshal(smartify(this), mode, ostream) ;
        }
        return size ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalObjectArray(array<IDataStructure> objects, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        int size = 0 ;

        // Write length of array
        if( mode == IMarshaller::MARSHAL_WRITE )
        {
            // Null object marker
            dos->writeBoolean( objects != NULL ) ;

            // Check for NULL array
            if( objects != NULL )
                dos->writeShort( (short)objects.size() ) ;
            else
                return BOOLSIZE ;
        }
        // Check for NULL array
        if( objects == NULL )
            return BOOLSIZE ;

        // Add size for null marker and array length
        size += BOOLSIZE ;
        size += sizeof(short) ;

        // Write/measure each object in array
        for( int i = 0; i < (int)objects.size(); i++ )
            size += objects[i]->marshal(smartify(this), mode, ostream) ;

        return size ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalByteArray(array<char> values, int mode, p<IOutputStream> ostream) throw(IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = checkOutputStream(ostream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        int size = 0 ;

        // Write length/content of array
        if( mode == IMarshaller::MARSHAL_WRITE )
        {
            // Null marker
            dos->writeBoolean( values != NULL ) ;

            // Check for NULL array
            if( values != NULL )
            {
                // Array length
                int length = (int)values.size() ;

                // Length and content
                dos->writeInt( length ) ;
                dos->write( values.c_array(), 0, length) ;
            }
        }
        // Check for NULL array
        if( values == NULL )
            return BOOLSIZE ;

        // Add size for null marker, array length and content
        size += BOOLSIZE ;
        size += sizeof(int) ;
        size += (int)values.size() ;

        return size ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::marshalMap(p<PropertyMap> object, int mode, p<IOutputStream> ostream) throw(IOException)
{
    if( !formatInfo->getTightEncodingEnabled() )
    {
        int size = 0 ;

        // Check for NULL map
        if( object == NULL )
            return sizeof(int) ;

        PropertyMap::iterator tempIter ;
        string        key ;
        MapItemHolder val ;

        // Add size for map item count
        size += sizeof(int) ;

        // Calculate size of map
        if( mode == IMarshaller::MARSHAL_SIZE )
        {
            // Loop through map contents
            for( tempIter = object->begin() ;
                tempIter != object->end() ;
                tempIter++ )
            {
                array<char> buffer ;

                // Extract key-value
                key = tempIter->first ;
                val = tempIter->second ;

                // Add size for key char count, lenght of key and value type
                size += sizeof(short) ;
                size += ( encoder != NULL ) ? encoder->length( p<string>( new string(key)) ) : (int)key.length() ;
                size += sizeof(unsigned char) ;

                // Write the map value
                switch( val.getType() )
                {
                    case MapItemHolder::BOOLEAN:
                        size += BOOLSIZE ;
                        break ;
                    case MapItemHolder::BYTE:
                        size += sizeof(char) ;
                        break ;
                    case MapItemHolder::BYTEARRAY:
                        buffer = val.getBytes() ;
                        size += (int)buffer.size() ;
                        break ;
                    case MapItemHolder::DOUBLE:
                        size += sizeof(double) ;
                        break ;
                    case MapItemHolder::FLOAT:
                        size += sizeof(float) ;
                        break ;
                    case MapItemHolder::INTEGER:
                        size += sizeof(int) ;
                        break ;
                    case MapItemHolder::LONG:
                        size += sizeof(long) ;
                        break ;
                    case MapItemHolder::SHORT:
                        size += sizeof(short) ;
                        break ;
                    default:
                        // Calculate encoded/decoded string length
                        size += ( encoder != NULL ) ? encoder->length(val.getString()) : (int)val.getString()->size() ;
                }
            }
        }

        // Write size/content of map
        else if( mode == IMarshaller::MARSHAL_WRITE )
        {
            // Assert that supplied output stream is a data output stream
            p<DataOutputStream> dos = checkOutputStream(ostream) ;

            // Write 'null' marker
            if( object == NULL )
            {
                dos->writeInt(-1) ;
                return size ;
            }

            // Write map item count
            dos->writeInt( (int)object->size()) ;

            // Loop through map contents
            for( tempIter = object->begin() ;
                tempIter != object->end() ;
                tempIter++ )
            {
                array<char> buffer ;

                // Extract key-value
                key = tempIter->first ;
                val = tempIter->second ;

                // Add size for key char count and value type
                size += sizeof(short) ;
                size += sizeof(unsigned char) ;

                // Write the map key, add size for key length
                size += dos->writeString( p<string>( new string(tempIter->first) ) ) ;

                // Write the map value
                switch( val.getType() )
                {
                    case MapItemHolder::BOOLEAN:
                        dos->writeByte( TYPE_BOOLEAN ) ;
                        dos->writeBoolean( val.getBoolean() ) ;
                        size += BOOLSIZE ;
                        break ;
                    case MapItemHolder::BYTE:
                        dos->writeByte( TYPE_BYTE ) ;
                        dos->writeByte( val.getByte() ) ;
                        size += sizeof(char) ;
                        break ;
                    case MapItemHolder::BYTEARRAY:
                        dos->writeByte( TYPE_BYTEARRAY ) ;
                        buffer = val.getBytes() ;
                        dos->writeInt( (int)buffer.size() ) ;
                        dos->write(buffer.c_array(), 0, (int)buffer.size()) ;
                        size += (int)buffer.size() ;
                        break ;
                    case MapItemHolder::DOUBLE:
                        dos->writeByte( TYPE_DOUBLE ) ;
                        dos->writeDouble( val.getDouble() ) ;
                        size += sizeof(double) ;
                        break ;
                    case MapItemHolder::FLOAT:
                        dos->writeByte( TYPE_FLOAT ) ;
                        dos->writeFloat( val.getFloat() ) ;
                        size += sizeof(float) ;
                        break ;
                    case MapItemHolder::INTEGER:
                        dos->writeByte( TYPE_INTEGER ) ;
                        dos->writeInt( val.getInt() ) ;
                        size += sizeof(int) ;
                        break ;
                    case MapItemHolder::LONG:
                        dos->writeByte( TYPE_LONG ) ;
                        dos->writeLong( val.getLong() ) ;
                        size += sizeof(long) ;
                        break ;
                    case MapItemHolder::SHORT:
                        dos->writeByte( TYPE_SHORT ) ;
                        dos->writeShort( val.getShort() ) ;
                        size += sizeof(short) ;
                        break ;
                    case MapItemHolder::STRING:
                        dos->writeByte( TYPE_STRING ) ;
                        size += dos->writeString( val.getString() ) ;
                        break ;
                    default:
                        dos->writeByte( TYPE_NULL ) ;
                }
            }
        }
        return size ;
    }
    else
    {
        // Not yet implemented (tight marshalling)
    }
    return 0 ;
}

/*
 * 
 */
bool OpenWireMarshaller::unmarshalBoolean(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        return dis->readBoolean() ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return 0 ;
}

/*
 * 
 */
char OpenWireMarshaller::unmarshalByte(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        return dis->readByte() ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return 0 ;
}
/*
 * 
 */
short OpenWireMarshaller::unmarshalShort(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        return dis->readShort() ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return 0 ;
}

/*
 * 
 */
int OpenWireMarshaller::unmarshalInt(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        return dis->readInt() ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return 0 ;
}

/*
 * 
 */
long long OpenWireMarshaller::unmarshalLong(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        return dis->readLong() ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return 0 ;
}

/*
 * 
 */
float OpenWireMarshaller::unmarshalFloat(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        return dis->readFloat() ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return 0 ;
}

/*
 * 
 */
double OpenWireMarshaller::unmarshalDouble(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        return dis->readFloat() ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return 0 ;
}

/*
 * 
 */
p<string> OpenWireMarshaller::unmarshalString(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        if( dis->readBoolean() )
            return dis->readString() ;
        else
            return NULL ;
    }
    else
    {
        // Not yet implemented (loose unmarshalling)
    }
    return NULL ;
}

/*
 * 
 */
p<IDataStructure> OpenWireMarshaller::unmarshalObject(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        // Null marker
        if( !dis->readBoolean() )
            return NULL ;

        // Read data structure
        unsigned char dataType = dis->readByte() ;

        // Create command object
        p<IDataStructure> object = BaseDataStructure::createObject(dataType) ;
        if( object == NULL )
            throw IOException("Unmarshal failed; unknown data structure type %d, at %s line %d", dataType, __FILE__, __LINE__) ;

        // Finally, unmarshal command body
        object->unmarshal(smartify(this), IMarshaller::MARSHAL_READ, istream) ;
        return object ;
    }
    else
    {
        // Not yet implemented (tight unmarshalling)
    }
    return NULL ;
}

/*
 * 
 */
array<IDataStructure> OpenWireMarshaller::unmarshalObjectArray(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        // Null marker
        if( !dis->readBoolean() )
            return NULL ;

        int length = dis->readShort() ;

        // Check for NULL array
        if( length == 0 )
        {
            return NULL;
        }

        // Create array
        array<IDataStructure> objects (length) ;

        // Unmarshal each item in array
        for( int i = 0 ; i < length ; i++ )
            objects[i] = unmarshalObject(mode, istream) ;

        return objects ;
    }
    else
    {
        // Not yet implemented (loose unmarshalling)
    }
    return NULL;
}

/*
 * 
 */
array<char> OpenWireMarshaller::unmarshalByteArray(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        // Null marker
        if( !dis->readBoolean() )
            return NULL ;

        int length = dis->readInt() ;

        // Check for NULL array
        if( length == 0 )
            return NULL ;

        // Create array
        array<char> value (length);

        // Unmarshal all bytes in array
        dis->read(value.c_array(), 0, length) ;

        return value ;
    }
    else
    {
        // Not yet implemented (loose unmarshalling)
    }
    return NULL ;
}

/*
 * 
 */
p<PropertyMap> OpenWireMarshaller::unmarshalMap(int mode, p<IInputStream> istream) throw(IOException)
{
    // Assert that supplied input stream is a data input stream
    p<DataInputStream> dis = checkInputStream(istream) ;

    if( !formatInfo->getTightEncodingEnabled() )
    {
        // Get size of map
        int size = dis->readInt() ;

        // Check for NULL map
        if( size < 0 )
            return NULL ;

        // Create map
        p<PropertyMap> object = new PropertyMap() ;
        p<string>      key ;
        MapItemHolder  val ;
        array<char>    buffer ;
        unsigned char  type ;
        int            length ;

        // Loop through and read all key-values
        for( int i = 0 ; i < size ; i++ )
        {
            // Get next key
            key = dis->readString() ;
            
            // Get the primitive type
            type = dis->readByte() ;

            // Depending on type read next value
            switch( type )
            {
                case TYPE_BOOLEAN:
                    val = MapItemHolder( dis->readBoolean() ) ;
                    break ;
                case TYPE_BYTE:
                    val = MapItemHolder( dis->readByte() ) ;
                    break ;
                case TYPE_BYTEARRAY:
                    length = dis->readInt() ;
                    buffer = array<char> (length) ;
                    dis->read(buffer.c_array(), 0, length) ;
                    val = MapItemHolder( buffer ) ;
                    break ;
                case TYPE_DOUBLE:
                    val = MapItemHolder( dis->readDouble() ) ;
                    break ;
                case TYPE_FLOAT:
                    val = MapItemHolder( dis->readFloat() ) ;
                    break ;
                case TYPE_INTEGER:
                    val = MapItemHolder( dis->readInt() ) ;
                    break ;
                case TYPE_LONG:
                    val = MapItemHolder( dis->readLong() ) ;
                    break ;
                case TYPE_SHORT:
                    val = MapItemHolder( dis->readShort() ) ;
                    break ;
                case TYPE_STRING:
                    val = MapItemHolder( dis->readString() ) ;
                    break ;
                default:
                    val = MapItemHolder() ;
            }
            // Insert value into property map
            (*object)[key->c_str()] = val ;
        }
        return object ;
    }
    else
    {
        // Not yet implemented (loose unmarshalling)
    }
    return NULL;
}

/*
 * 
 */
p<DataOutputStream> OpenWireMarshaller::checkOutputStream(p<IOutputStream> ostream) throw (IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataOutputStream> dos = p_dyncast<DataOutputStream> (ostream) ;
    if( dos == NULL )
        throw IOException("OpenWireMarshaller requires a DataOutputStream") ;

    return dos ;
}

/*
 * 
 */
p<DataInputStream> OpenWireMarshaller::checkInputStream(p<IInputStream> istream) throw (IOException)
{
    // Assert that supplied output stream is a data output stream
    p<DataInputStream> dis = p_dyncast<DataInputStream> (istream) ;
    if( dis == NULL )
        throw IOException("OpenWireMarshaller requires a DataInputStream") ;

    return dis ;
}
