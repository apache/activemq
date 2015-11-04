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
package org.apache.activemq.transport.amqp.client.util;

import java.util.Date;
import java.util.HashMap;

public final class TypeConversionSupport {

    static class ConversionKey {
        final Class<?> from;
        final Class<?> to;
        final int hashCode;

        public ConversionKey(Class<?> from, Class<?> to) {
            this.from = from;
            this.to = to;
            this.hashCode = from.hashCode() ^ (to.hashCode() << 1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || o.getClass() != this.getClass()) {
                return false;
            }

            ConversionKey x = (ConversionKey) o;
            return x.from == from && x.to == to;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    interface Converter {
        Object convert(Object value);
    }

    private static final HashMap<ConversionKey, Converter> CONVERSION_MAP = new HashMap<ConversionKey, Converter>();

    static {
        Converter toStringConverter = new Converter() {
            @Override
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
            @Override
            public Object convert(Object value) {
                return Boolean.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Byte.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Short.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Integer.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Long.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Float.valueOf((String) value);
            }
        });
        CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Double.valueOf((String) value);
            }
        });

        Converter longConverter = new Converter() {
            @Override
            public Object convert(Object value) {
                return Long.valueOf(((Number) value).longValue());
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConverter);
        CONVERSION_MAP.put(new ConversionKey(Date.class, Long.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Long.valueOf(((Date) value).getTime());
            }
        });

        Converter intConverter = new Converter() {
            @Override
            public Object convert(Object value) {
                return Integer.valueOf(((Number) value).intValue());
            }
        };
        CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), intConverter);
        CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), intConverter);

        CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return Short.valueOf(((Number) value).shortValue());
            }
        });

        CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), new Converter() {
            @Override
            public Object convert(Object value) {
                return new Double(((Number) value).doubleValue());
            }
        });
    }

    public static Object convert(Object value, Class<?> toClass) {

        assert value != null && toClass != null;

        if (value.getClass() == toClass) {
            return value;
        }

        Class<?> fromClass = value.getClass();

        if (fromClass.isPrimitive()) {
            fromClass = convertPrimitiveTypeToWrapperType(fromClass);
        }

        if (toClass.isPrimitive()) {
            toClass = convertPrimitiveTypeToWrapperType(toClass);
        }

        Converter c = CONVERSION_MAP.get(new ConversionKey(fromClass, toClass));
        if (c == null) {
            return null;
        }

        return c.convert(value);
    }

    private static Class<?> convertPrimitiveTypeToWrapperType(Class<?> type) {
        Class<?> rc = type;
        if (type.isPrimitive()) {
            if (type == int.class) {
                rc = Integer.class;
            } else if (type == long.class) {
                rc = Long.class;
            } else if (type == double.class) {
                rc = Double.class;
            } else if (type == float.class) {
                rc = Float.class;
            } else if (type == short.class) {
                rc = Short.class;
            } else if (type == byte.class) {
                rc = Byte.class;
            } else if (type == boolean.class) {
                rc = Boolean.class;
            }
        }

        return rc;
    }

    private TypeConversionSupport() {}
}
