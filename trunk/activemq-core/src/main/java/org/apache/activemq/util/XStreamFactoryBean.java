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

import java.util.Iterator;
import java.util.Map;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.propertyeditors.ClassEditor;
import org.springframework.util.Assert;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.ConverterMatcher;
import com.thoughtworks.xstream.converters.SingleValueConverter;

public class XStreamFactoryBean implements FactoryBean {

	XStream xstream = new XStream();
	
    /**
     * Sets the <code>Converters</code> or <code>SingleValueConverters</code> to be registered with the
     * <code>XStream</code> instance.
     *
     * @see Converter
     * @see SingleValueConverter
     */
    public void setConverters(ConverterMatcher[] converters) {
        for (int i = 0; i < converters.length; i++) {
            if (converters[i] instanceof Converter) {
                xstream.registerConverter((Converter) converters[i], i);
            }
            else if (converters[i] instanceof SingleValueConverter) {
                xstream.registerConverter((SingleValueConverter) converters[i], i);
            }
            else {
                throw new IllegalArgumentException("Invalid ConverterMatcher [" + converters[i] + "]");
            }
        }
    }

    /**
     * Set a alias/type map, consisting of string aliases mapped to <code>Class</code> instances (or Strings to be
     * converted to <code>Class</code> instances).
     *
     * @see org.springframework.beans.propertyeditors.ClassEditor
     */
    public void setAliases(Map aliases) {
        for (Iterator iterator = aliases.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry entry = (Map.Entry) iterator.next();
            // Check whether we need to convert from String to Class.
            Class type;
            if (entry.getValue() instanceof Class) {
                type = (Class) entry.getValue();
            }
            else {
                ClassEditor editor = new ClassEditor();
                editor.setAsText(String.valueOf(entry.getValue()));
                type = (Class) editor.getValue();
            }
            xstream.alias((String) entry.getKey(), type);
        }
    }	
    
    /**
     * Sets the XStream mode.
     *
     * @see XStream#XPATH_REFERENCES
     * @see XStream#ID_REFERENCES
     * @see XStream#NO_REFERENCES
     */
    public void setMode(int mode) {
        xstream.setMode(mode);
    }    
    
    /**
     * Sets the classes, for which mappings will be read from class-level JDK 1.5+ annotation metadata.
     *
     * @see Annotations#configureAliases(XStream, Class[])
     */
    public void setAnnotatedClass(Class<?> annotatedClass) {
        Assert.notNull(annotatedClass, "'annotatedClass' must not be null");
        xstream.processAnnotations(annotatedClass);
    }

    /**
     * Sets annotated classes, for which aliases will be read from class-level JDK 1.5+ annotation metadata.
     *
     * @see Annotations#configureAliases(XStream, Class[])
     */
    public void setAnnotatedClasses(Class<?>[] annotatedClasses) {
        Assert.notEmpty(annotatedClasses, "'annotatedClasses' must not be empty");
        xstream.processAnnotations(annotatedClasses);
    }    
	
	public Object getObject() throws Exception {
		return xstream;
	}

	public Class getObjectType() {
		return XStream.class;
	}

	public boolean isSingleton() {
		return true;
	}

}
