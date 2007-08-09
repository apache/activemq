/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.util;

import java.util.Date;
import java.util.HashMap;

import org.apache.activemq.command.ActiveMQDestination;

public class TypeConversionSupport {

    static class ConversionKey {
        final Class from;
        final Class to;
        final int hashCode;

        public ConversionKey(Class from, Class to) {
            this.from = from;
            this.to = to;
            this.hashCode = from.hashCode() ^ (to.hashCode() << 1);
        }

        public boolean equals(Object o) {
            ConversionKey x = (ConversionKey) o;
            return x.from == from && x.to == to;
        }

        public int hashCode() {
            return hashCode;
        }
    }

    interface Converter {
        Object convert(Object value);
    }

    private static final HashMap CONVERSION_MAP = new HashMap();
    static {
        Converter toStringConverter = new Converter() {
            public Object convert(Object value) {
                return value.toString();
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Boolean.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Byte.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Long.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Float.class, String.class), toStringConverter);
        CONVERSION_MAP.put(new ConversionKey(Double.class, String.class), toStringConverter);

        CONVERSION_MAP.put(new ConversionKey(String.class, Boolean.class), new Converter() {
            public Object convert(Object value) {
                return Boolean.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), new Converter() {
            public Object convert(Object value) {
                return Byte.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), new Converter() {
            public Object convert(Object value) {
                return Short.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), new Converter() {
            public Object convert(Object value) {
                return Integer.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), new Converter() {
            public Object convert(Object value) {
                return Long.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), new Converter() {
            public Object convert(Object value) {
                return Float.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), new Converter() {
            public Object convert(Object value) {
                return Double.valueOf((String) value);
            }
        });

        Converter longConverter = new Converter() {
            public Object convert(Object value) {
                return Long.valueOf(((Number) value).longValue());
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Date.class, Long.class), new Converter() {
            public Object convert(Object value) {
                return Long.valueOf(((Date) value).getTime());
            }
        });

        Converter intConverter = new Converter() {
            public Object convert(Object value) {
                return Integer.valueOf(((Number) value).intValue());
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), intConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), intConverter);

        CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), new Converter() {
            public Object convert(Object value) {
                return Short.valueOf(((Number) value).shortValue());
            }
        });
        
        CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), new Converter() {
            public Object convert(Object value) {
                return new Double(((Number) value).doubleValue());
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, ActiveMQDestination.class), new Converter() {
            public Object convert(Object value) {
                return ActiveMQDestination.createDestination((String) value, ActiveMQDestination.QUEUE_TYPE);
            }
        });
    }

    public static Object convert(Object value, Class clazz) {

        assert value != null && clazz != null;

        if (value.getClass() == clazz) {
            return value;
        }

        Converter c = (Converter) CONVERSION_MAP.get(new ConversionKey(value.getClass(), clazz));
        if (c == null) {
            return null;
        }
        return c.convert(value);

    }

}
