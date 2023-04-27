/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.objectweb.jtests.jms.admin;

import java.util.Properties;

public class AdminFactory
{
   private static final String PROP_NAME = "jms.provider.admin.class";

   protected static String getAdminClassName(final Properties props)
   {
      String adminClassName = props.getProperty(AdminFactory.PROP_NAME);
      return adminClassName;
   }

   public static Admin getAdmin(final Properties props)
   {
      String adminClassName = AdminFactory.getAdminClassName(props);
      Admin admin = null;
      if (adminClassName == null)
      {
         throw new RuntimeException("Property " + AdminFactory.PROP_NAME + " has not been found in input props");
      }
      try
      {
         Class adminClass = Class.forName(adminClassName);
         admin = (Admin)adminClass.newInstance();
      }
      catch (ClassNotFoundException e)
      {
         throw new RuntimeException("Class " + adminClassName + " not found.", e);
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
      return admin;
   }
}
