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

import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.config.ConfigurationException;
import org.apache.shiro.config.Ini;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 5.10.0
 */
public class IniEnvironmentTest {

    IniEnvironment env;

    @Before
    public void setUp() {
        env = new IniEnvironment();
    }

    protected void authenticate() {
        authenticate("foo", "bar");
    }

    protected void authenticate(String username, String password) {
        Subject subject = new Subject.Builder(env.getSecurityManager()).buildSubject();
        subject.login(new UsernamePasswordToken(username, password));
    }

    @Test
    public void testIniInstanceConstructorArg() {
        Ini ini = new Ini();
        ini.addSection("users").put("foo", "bar");
        env = new IniEnvironment(ini);
        authenticate();
    }

    @Test
    public void testStringConstructorArg() {
        String config =
                "[users]\n" +
                "foo = bar";

        env = new IniEnvironment(config);
        authenticate();
    }

    @Test
    public void testSetIni() {
        Ini ini = new Ini();
        ini.addSection("users").put("foo", "bar");

        env = new IniEnvironment();
        env.setIni(ini);
        env.init();

        authenticate();
    }

    @Test
    public void testSetIniString() {
        String config =
                "[users]\n" +
                "foo = bar";

        env = new IniEnvironment();
        env.setIniConfig(config);
        env.init();

        authenticate();
    }

    @Test
    public void testSetIniResourcePath() {
        env = new IniEnvironment();
        env.setIniResourcePath("classpath:minimal.shiro.ini");
        env.init();

        authenticate("system", "manager");
    }

    @Test
    public void testDefaultClasspathIni() {
        env = new IniEnvironment();
        env.init();

        authenticate("system", "manager");
    }

    @Test(expected = ConfigurationException.class)
    public void testNoDefaultClasspathIni() {
        env = new IniEnvironment() {
            @Override
            protected void apply(Ini ini) {
                super.apply(ini);
                //clear out the objects to simulate as if the ini file wasn't found:
                this.objects.clear();
            }
        };
        env.init();
        authenticate("system", "manager");
    }

}
