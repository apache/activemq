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

#include "marshal/BaseDataStreamMarshaller.h"
#include "marshal/BrokerIdMarshaller.h"
#include "marshal/BrokerInfoMarshaller.h"
#include "command/AbstractCommand.h"

#include "command/BrokerId.h"
#include "command/BrokerInfo.h"
#include "command/IDataStructure.h"

using namespace ActiveMQ::Marshalling;
using namespace ActiveMQ::Command;
using namespace ActiveMQ::IO;
using std::auto_ptr;
using boost::shared_ptr;

shared_ptr<BrokerError>
BaseDataStreamMarshaller::unmarshalBrokerError(ProtocolFormat& wireFormat,
                                               BinaryReader& dataIn,
                                               BooleanStream& bs) {
    return shared_ptr<BrokerError>();
}

auto_ptr<IDataStructure>
BaseDataStreamMarshaller::unmarshalCachedObject(ProtocolFormat& wireFormat,
                                                BinaryReader& dataIn,
                                                BooleanStream& bs) {
    return unmarshalNestedObject(wireFormat, dataIn, bs);
}

auto_ptr<IDataStructure>
BaseDataStreamMarshaller::unmarshalNestedObject(ProtocolFormat& wireFormat,
                                                BinaryReader& dataIn,
                                                BooleanStream& bs) {
    if (bs.readBoolean()) {
        unsigned char type = dataIn.readByte();
        BaseDataStreamMarshaller *marsh = wireFormat.getMarshaller(type);
        if (marsh == NULL)
            return auto_ptr<IDataStructure>(NULL);

        auto_ptr<IDataStructure> ret(marsh->createCommand());
        BaseCommand *test = dynamic_cast<BaseCommand*>(ret.get());
        if (test && test->isMarshalAware())
            bs.readBoolean(); // don't care what this is; should always be false since we don't do caching
        marsh->unmarshal(wireFormat,
                         *ret,
                         dataIn,
                         bs);
        return ret;
    }
    else
        return auto_ptr<IDataStructure>(NULL);
}

size_t
BaseDataStreamMarshaller::marshal1BrokerError(ProtocolFormat& wireFormat,
                                              const shared_ptr<const BrokerError>& o,
                                              BooleanStream& bs) {
    return 0;
}

void
BaseDataStreamMarshaller::marshal2BrokerError(ProtocolFormat& wireFormat,
                                              const shared_ptr<const BrokerError>& o,
                                              BinaryWriter& dataOut,
                                              BooleanStream& bs) {
    
}

size_t
BaseDataStreamMarshaller::marshal1CachedObject(ProtocolFormat& wireFormat,
                                               const AbstractCommand& o,
                                               BooleanStream& bs) {

    return marshal1NestedObject(wireFormat, o, bs);
}

void
BaseDataStreamMarshaller::marshal2CachedObject(ProtocolFormat& wireFormat,
                                               const AbstractCommand& o,
                                               BinaryWriter& dataOut,
                                               BooleanStream& bs) {

    marshal2NestedObject(wireFormat, o, dataOut, bs);
}

size_t
BaseDataStreamMarshaller::marshal1NestedObject(ProtocolFormat& wireFormat,
                                               const AbstractCommand& o,
                                               BooleanStream& bs) {
    bs.writeBoolean(true);
    return 1 + wireFormat.getMarshaller(o.getCommandType())->marshal1(wireFormat,
                                                                      o,
                                                                      bs);
}

void
BaseDataStreamMarshaller::marshal2NestedObject(ProtocolFormat& wireFormat,
                                               const AbstractCommand& o,
                                               BinaryWriter& dataOut,
                                               BooleanStream& bs) {
    if (bs.readBoolean()) {
        unsigned char type = o.getCommandType();
        dataOut.write(&type, 1);

        wireFormat.getMarshaller(type)->marshal2(wireFormat,
                                                 o,
                                                 dataOut,
                                                 bs);
    }
}


size_t
BaseDataStreamMarshaller::marshal1CachedObject(ProtocolFormat& wireFormat,
                                               const shared_ptr<const IDataStructure>& o,
                                               BooleanStream& bs) {

    return marshal1NestedObject(wireFormat, o, bs);
}

void
BaseDataStreamMarshaller::marshal2CachedObject(ProtocolFormat& wireFormat,
                                               const shared_ptr<const IDataStructure>& o,
                                               BinaryWriter& dataOut,
                                               BooleanStream& bs) {

    marshal2NestedObject(wireFormat, o, dataOut, bs);
}

size_t
BaseDataStreamMarshaller::marshal1NestedObject(ProtocolFormat& wireFormat,
                                               const shared_ptr<const IDataStructure>& o,
                                               BooleanStream& bs) {

    bs.writeBoolean(o != NULL);
    if (o != NULL) {
        return 1 + wireFormat.getMarshaller(o->getCommandType())->marshal1(wireFormat,
                                                                           *o,
                                                                           bs);
    }
    
    return 0;    
}

void
BaseDataStreamMarshaller::marshal2NestedObject(ProtocolFormat& wireFormat,
                                               const shared_ptr<const IDataStructure>& o,
                                               BinaryWriter& dataOut,
                                               BooleanStream& bs) {

    if (bs.readBoolean()) {
        unsigned char type = o->getCommandType();
        dataOut.write(&type, 1);
        wireFormat.getMarshaller(type)->marshal2(wireFormat,
                                                                *o,
                                                                dataOut,
                                                                bs);
    }
}


size_t
BaseDataStreamMarshaller::marshal1CachedObject(ProtocolFormat& wireFormat,
                                               const IDataStructure& o,
                                               BooleanStream& bs) {
    return marshal1NestedObject(wireFormat, o, bs);
}

void
BaseDataStreamMarshaller::marshal2CachedObject(ProtocolFormat& wireFormat,
                                               const IDataStructure& o,
                                               BinaryWriter& dataOut,
                                               BooleanStream& bs) {

    marshal2NestedObject(wireFormat, o, dataOut, bs);
}

size_t
BaseDataStreamMarshaller::marshal1NestedObject(ProtocolFormat& wireFormat,
                                               const IDataStructure& o,
                                               BooleanStream& bs) {
    return marshal1NestedObject(wireFormat, o, bs);
}

void
BaseDataStreamMarshaller::marshal2NestedObject(ProtocolFormat& wireFormat,
                                               const IDataStructure& o,
                                               BinaryWriter& dataOut,
                                               BooleanStream& bs) {
    marshal2NestedObject(wireFormat, o, dataOut, bs);
}

namespace ActiveMQ {
  namespace Marshalling {

      template<>
      void
      BaseDataStreamMarshaller::marshal2ObjectArray(ProtocolFormat& wireFormat,
                                                    const vector<shared_ptr<const BrokerId> >& arr,
                                                    BinaryWriter& dataOut,
                                                    BooleanStream& bs) {
          if (bs.readBoolean()) {
              dataOut.writeShort(arr.size());
              for (vector<shared_ptr<const BrokerId> >::const_iterator i = arr.begin();
                   i != arr.end();
                   ++i)
                  marshal2NestedObject(wireFormat,
                                       *i,
                                       dataOut,
                                       bs);
          }
      }

      template<>
      void
      BaseDataStreamMarshaller::marshal2ObjectArray(ProtocolFormat& wireFormat,
                                                    const vector<shared_ptr<const IDataStructure> >& arr,
                                                    BinaryWriter& dataOut,
                                                    BooleanStream& bs) {

          if (bs.readBoolean()) {
              dataOut.writeShort(arr.size());
              for (vector<shared_ptr<const IDataStructure> >::const_iterator i = arr.begin();
                   i != arr.end();
                   ++i)
                  marshal2NestedObject(wireFormat,
                                       *i,
                                       dataOut,
                                       bs);
          }
      }

      template<>
      void
      BaseDataStreamMarshaller::marshal2ObjectArray(ProtocolFormat& wireFormat,
                                                    const vector<shared_ptr<const BrokerInfo> >& arr,
                                                    BinaryWriter& dataOut,
                                                    BooleanStream& bs) {

          if (bs.readBoolean()) {
              dataOut.writeShort(arr.size());
              for (vector<shared_ptr<const BrokerInfo> >::const_iterator i = arr.begin();
                   i != arr.end();
                   ++i)
                  marshal2NestedObject(wireFormat,
                                       *i,
                                       dataOut,
                                       bs);
          }
      }

      template<>
      size_t
      BaseDataStreamMarshaller::marshal1ObjectArray(ProtocolFormat& wireFormat,
                                                    const vector<shared_ptr<const BrokerId> >& arr,
                                                    BooleanStream& bs) {

          unsigned int ret = 0;
          if (!arr.empty()) {
              bs.writeBoolean(true);
              ret += 2;

              for (vector<shared_ptr<const BrokerId> >::const_iterator i = arr.begin();
                   i != arr.end(); ++i) {
                  ret += marshal1NestedObject(wireFormat, *i, bs);
              }
          }
          else
              bs.writeBoolean(false);
    
          return ret;
      }

      template<>
      size_t
      BaseDataStreamMarshaller::marshal1ObjectArray(ProtocolFormat& wireFormat,
                                                    const vector<shared_ptr<const IDataStructure> >& arr,
                                                    BooleanStream& bs) {

          return 0;
      }

      template<>
      size_t
      BaseDataStreamMarshaller::marshal1ObjectArray(ProtocolFormat& wireFormat,
                                                    const vector<shared_ptr<const BrokerInfo> >& arr,
                                                    BooleanStream& bs) {


          unsigned int ret = 0;
          if (!arr.empty()) {
              bs.writeBoolean(true);
              ret += 2;

              for (vector<shared_ptr<const BrokerInfo> >::const_iterator i = arr.begin();
                   i != arr.end(); ++i) {
                  ret += marshal1NestedObject(wireFormat, *(*i), bs);
              }
          }
          else
              bs.writeBoolean(false);
    
          return ret;
      }

  };
};

int64_t
BaseDataStreamMarshaller::unmarshalLong(ProtocolFormat& wireFormat, BinaryReader& dataIn, BooleanStream& bs) {
    if (bs.readBoolean()) {
        if (bs.readBoolean()) {
            return dataIn.readLong();
        }
        else {
            return (int64_t)dataIn.readInt();
        }
    }
    else {
        if (bs.readBoolean()) {
            return (int64_t)dataIn.readShort();
        }
        else
            return 0;
    }
}

size_t
BaseDataStreamMarshaller::writeLong1(int64_t o, BooleanStream& bs) {
    if( o == 0 ) {
        bs.writeBoolean(false);
        bs.writeBoolean(false);
        return 0;
    } else if ( (o & 0xFFFFFFFFFFFF0000LL ) == 0 ) {
        bs.writeBoolean(false);
        bs.writeBoolean(true);            
        return 2;
    } else if ( (o & 0xFFFFFFFF00000000LL ) == 0) {
        bs.writeBoolean(true);
        bs.writeBoolean(false);
        return 4;
    } else {
        bs.writeBoolean(true);
        bs.writeBoolean(true);
        return 8;
    }
}

void
BaseDataStreamMarshaller::writeLong2(int64_t o, BinaryWriter& dataOut, BooleanStream& bs) {
    if( bs.readBoolean() ) {
        if( bs.readBoolean() ) {
            dataOut.writeLong(o);
        } else {
            dataOut.writeInt((int) o);
        }
    } else {
        if( bs.readBoolean() ) {
            dataOut.writeShort((int) o);
        }
    }
}

size_t
BaseDataStreamMarshaller::writeString1(const std::string& str, BooleanStream& bs) {
    size_t rc = 0;

    if (!str.empty()) {
        bs.writeBoolean(true);
        rc += 2;
        rc += str.length();
        bs.writeBoolean(true);
    }
    else
        bs.writeBoolean(false);
    return rc;
}

void
BaseDataStreamMarshaller::writeString2(const std::string& str, BinaryWriter& dataOut,
                                       BooleanStream& bs) {
    if (bs.readBoolean()) {
        if (bs.readBoolean()) {
            dataOut.writeString(str);
        }
    }
}

vector<uint8_t>
BaseDataStreamMarshaller::unmarshalConstByteSequence(ProtocolFormat& wf,
                                                     BinaryReader& dataIn,
                                                     BooleanStream& bs,
                                                     size_t num) {
    vector<uint8_t> ret;
    ret.resize(num);
    dataIn.read(&(ret[0]), num);
    return ret;
}

vector<uint8_t>
BaseDataStreamMarshaller::unmarshalByteSequence(ProtocolFormat& wf,
                                                BinaryReader& dataIn,
                                                BooleanStream& bs) {
    vector<uint8_t> ret;
    if (bs.readBoolean()) {
        int size = dataIn.readInt();
        ret.resize(size);
        dataIn.read(&(ret[0]), size);
    }
    return ret;
}

std::string
BaseDataStreamMarshaller::unmarshalString(ProtocolFormat& wireFormat,
                                          ActiveMQ::IO::BinaryReader& dataIn,
                                          ActiveMQ::IO::BooleanStream& bs) {
    if (bs.readBoolean()) {
        bs.readBoolean(); // don't care about UTFness for now
        return dataIn.readString();
    }
    else
        return "";
}
