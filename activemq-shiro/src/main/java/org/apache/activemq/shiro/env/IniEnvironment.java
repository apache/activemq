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
package org.apache.activemq.shiro.env;

import org.apache.activemq.shiro.authz.ActiveMQPermissionResolver;
import org.apache.activemq.shiro.mgt.DefaultActiveMqSecurityManager;
import org.apache.shiro.ShiroException;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.env.DefaultEnvironment;
import org.apache.shiro.io.ResourceUtils;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.util.Initializable;
import org.apache.shiro.util.LifecycleUtils;

import java.util.Map;

/**
 * @since 5.10.0
 */
public class IniEnvironment extends DefaultEnvironment implements Initializable {

    private Ini ini;
    private String iniConfig;
    private String iniResourePath;

    public IniEnvironment() {
    }

    public IniEnvironment(Ini ini) {
        this.ini = ini;
        init();
    }

    public IniEnvironment(String iniConfig) {
        Ini ini = new Ini();
        ini.load(iniConfig);
        this.ini = ini;
        init();
    }

    public void setIni(Ini ini) {
        this.ini = ini;
    }

    public void setIniConfig(String config) {
        this.iniConfig = config;
    }

    public void setIniResourcePath(String iniResourcePath) {
        this.iniResourePath = iniResourcePath;
    }

    @Override
    public void init() throws ShiroException {
        //this.environment and this.securityManager are null.  Try Ini config:
        Ini ini = this.ini;
        if (ini != null) {
            apply(ini);
        }

        if (this.objects.isEmpty() && this.iniConfig != null) {
            ini = new Ini();
            ini.load(this.iniConfig);
            apply(ini);
        }

        if (this.objects.isEmpty() && this.iniResourePath != null) {
            ini = new Ini();
            ini.loadFromPath(this.iniResourePath);
            apply(ini);
        }

        if (this.objects.isEmpty()) {
            if (ResourceUtils.resourceExists("classpath:shiro.ini")) {
                ini = new Ini();
                ini.loadFromPath("classpath:shiro.ini");
                apply(ini);
            }
        }

        if (this.objects.isEmpty()) {
            String msg = "Configuration error.  All heuristics for acquiring Shiro INI config " +
                    "have been exhausted.  Ensure you configure one of the following properties: " +
                    "1) ini 2) iniConfig 3) iniResourcePath and the Ini sections are not empty.";
            throw new ConfigurationException(msg);
        }

        LifecycleUtils.init(this.objects.values());
    }

    protected void apply(Ini ini) {
        if (ini != null && !ini.isEmpty()) {
            Map<String, ?> objects = createObjects(ini);
            this.ini = ini;
            this.objects.clear();
            this.objects.putAll(objects);
        }
    }

    private Map<String, ?> createObjects(Ini ini) {
        IniSecurityManagerFactory factory = new IniSecurityManagerFactory(ini) {

            @Override
            protected SecurityManager createDefaultInstance() {
                return new DefaultActiveMqSecurityManager();
            }

            @Override
            protected Realm createRealm(Ini ini) {
                IniRealm realm = (IniRealm)super.createRealm(ini);
                realm.setPermissionResolver(new ActiveMQPermissionResolver());
                return realm;
            }
        };
        factory.getInstance(); //trigger beans creation
        return factory.getBeans();
    }
}
