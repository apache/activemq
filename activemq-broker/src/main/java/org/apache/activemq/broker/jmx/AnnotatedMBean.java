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
package org.apache.activemq.broker.jmx;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.Principal;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.StandardMBean;
import javax.security.auth.Subject;

import org.apache.activemq.broker.util.AuditLogEntry;
import org.apache.activemq.broker.util.AuditLogService;
import org.apache.activemq.broker.util.JMXAuditLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MBean that looks for method/parameter descriptions in the Info annotation.
 */
public class AnnotatedMBean extends StandardMBean {

    private static final Map<String, Class<?>> primitives = new HashMap<String, Class<?>>();

    private static final Logger LOG = LoggerFactory.getLogger("org.apache.activemq.audit");

    private static final byte OFF = 0b00;
    private static final byte ENTRY = 0b01;
    private static final byte EXIT = 0b10;
    private static final byte ALL = 0b11;

    private static byte audit = OFF;
    private static AuditLogService auditLog;

    static {
        Class<?>[] p = { byte.class, short.class, int.class, long.class, float.class, double.class, char.class, boolean.class, };
        for (Class<?> c : p) {
            primitives.put(c.getName(), c);
        }
        audit = byteFromProperty("org.apache.activemq.audit");
        if (audit != OFF) {
            auditLog = AuditLogService.getAuditLog();
        }
    }

    private static byte byteFromProperty(String s) {
        byte val = OFF;
        String config = System.getProperty(s, "").toLowerCase(Locale.ENGLISH);
        if ("true".equals(config) || "entry".equals(config)) {
            val = ENTRY;
        } else if ("exit".equals(config)) {
            val = EXIT;
        } else if ("all".equals(config)) {
            val = ALL;
        }
        return val;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void registerMBean(ManagementContext context, Object object, ObjectName objectName) throws Exception {

        String mbeanName = object.getClass().getName() + "MBean";

        for (Class c : object.getClass().getInterfaces()) {
            if (mbeanName.equals(c.getName())) {
                context.registerMBean(new AnnotatedMBean(object, c), objectName);
                return;
            }
        }

        context.registerMBean(object, objectName);
    }

    /** Instance where the MBean interface is implemented by another object. */
    public <T> AnnotatedMBean(T impl, Class<T> mbeanInterface) throws NotCompliantMBeanException {
        super(impl, mbeanInterface);
    }

    /** Instance where the MBean interface is implemented by this object. */
    protected AnnotatedMBean(Class<?> mbeanInterface) throws NotCompliantMBeanException {
        super(mbeanInterface);
    }

    /** {@inheritDoc} */
    @Override
    protected String getDescription(MBeanAttributeInfo info) {

        String descr = info.getDescription();
        Method m = getMethod(getMBeanInterface(), "get" + info.getName().substring(0, 1).toUpperCase() + info.getName().substring(1));
        if (m == null)
            m = getMethod(getMBeanInterface(), "is" + info.getName().substring(0, 1).toUpperCase() + info.getName().substring(1));
        if (m == null)
            m = getMethod(getMBeanInterface(), "does" + info.getName().substring(0, 1).toUpperCase() + info.getName().substring(1));

        if (m != null) {
            MBeanInfo d = m.getAnnotation(MBeanInfo.class);
            if (d != null)
                descr = d.value();
        }
        return descr;
    }

    /** {@inheritDoc} */
    @Override
    protected String getDescription(MBeanOperationInfo op) {

        String descr = op.getDescription();
        Method m = getMethod(op);
        if (m != null) {
            MBeanInfo d = m.getAnnotation(MBeanInfo.class);
            if (d != null)
                descr = d.value();
        }
        return descr;
    }

    /** {@inheritDoc} */
    @Override
    protected String getParameterName(MBeanOperationInfo op, MBeanParameterInfo param, int paramNo) {
        String name = param.getName();
        Method m = getMethod(op);
        if (m != null) {
            for (Annotation a : m.getParameterAnnotations()[paramNo]) {
                if (MBeanInfo.class.isInstance(a))
                    name = MBeanInfo.class.cast(a).value();
            }
        }
        return name;
    }

    /**
     * Extracts the Method from the MBeanOperationInfo
     *
     * @param op
     *
     * @return a Method
     */
    private Method getMethod(MBeanOperationInfo op) {
        final MBeanParameterInfo[] params = op.getSignature();
        final String[] paramTypes = new String[params.length];
        for (int i = 0; i < params.length; i++)
            paramTypes[i] = params[i].getType();

        return getMethod(getMBeanInterface(), op.getName(), paramTypes);
    }

    /**
     * Returns the Method with the specified name and parameter types for the
     * given class, null if it doesn't exist.
     *
     * @param mbean
     * @param method
     * @param params
     *
     * @return a Method
     */
    private static Method getMethod(Class<?> mbean, String method, String... params) {
        try {
            final ClassLoader loader = mbean.getClassLoader();
            final Class<?>[] paramClasses = new Class<?>[params.length];
            for (int i = 0; i < params.length; i++) {
                paramClasses[i] = primitives.get(params[i]);
                if (paramClasses[i] == null)
                    paramClasses[i] = Class.forName(params[i], false, loader);
            }
            return mbean.getMethod(method, paramClasses);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Object invoke(String s, Object[] objects, String[] strings) throws MBeanException, ReflectionException {
        JMXAuditLogEntry entry = null;
        if (audit != OFF) {
            Subject subject = Subject.getSubject(AccessController.getContext());
            String caller = "anonymous";
            if (subject != null) {
                caller = "";
                for (Principal principal : subject.getPrincipals()) {
                    caller += principal.getName() + " ";
                }
            }

            entry = new JMXAuditLogEntry();
            entry.setUser(caller);
            entry.setTimestamp(System.currentTimeMillis());
            entry.setOperation(this.getMBeanInfo().getClassName() + "." + s);

            try
            {
               if (objects.length == strings.length)
               {
                  Method m = getMBeanMethod(this.getImplementationClass(), s, strings);
                  entry.getParameters().put("arguments", AuditLogEntry.sanitizeArguments(objects, m));
               }
               else
               {
                  // Supplied Method Signature and Arguments do not match.  Set all supplied Arguments in Log Entry.  To diagnose user error.
                  entry.getParameters().put("arguments", objects);
               }
            }
            catch (ReflectiveOperationException e)
            {
               // Method or Class not found, set all supplied arguments.  Set all supplied Arguments in Log Entry.  To diagnose user error.
               entry.getParameters().put("arguments", objects);
            }

            if ((audit&ENTRY) == ENTRY) {
                auditLog.log(entry);
            }
        }
        Object result = super.invoke(s, objects, strings);
        if ((audit&EXIT) == EXIT) {
            entry.complete();
            auditLog.log(entry);
        }
        return result;
    }

    private Method getMBeanMethod(Class clazz, String methodName, String[] signature) throws ReflectiveOperationException {
        Class[] parameterTypes = new Class[signature.length];
        for (int i = 0; i < signature.length; i++) {
            parameterTypes[i] = Class.forName(signature[i]);
        }
        return clazz.getMethod(methodName, parameterTypes);
    }
}
