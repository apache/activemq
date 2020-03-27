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
package org.apache.activemq.jaas;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoader.class);
    private static final Map<FileNameKey, ReloadableProperties> staticCache = new ConcurrentHashMap<FileNameKey, ReloadableProperties>();
    protected boolean debug;

    public void init(Map options) {
        debug = booleanOption("debug", options);
        if (debug) {
            LOG.debug("Initialized debug");
        }
    }

    public ReloadableProperties load(String nameProperty, String fallbackName, Map options) {
        FileNameKey key = new FileNameKey(nameProperty, fallbackName, options);
        key.setDebug(debug);

        ReloadableProperties result = staticCache.computeIfAbsent(key, k -> new ReloadableProperties(k));
        return result.obtained();
    }

    private static boolean booleanOption(String name, Map options) {
        return Boolean.parseBoolean((String) options.get(name));
    }

    public class FileNameKey {
        final File file;
        final String absPath;
        final boolean reload;
        private boolean decrypt;
        private boolean debug;
        private String algorithm;

        public FileNameKey(String nameProperty, String fallbackName, Map options) {
            this.file = new File(baseDir(options), stringOption(nameProperty, fallbackName, options));
            absPath = file.getAbsolutePath();
            reload = booleanOption("reload", options);
            decrypt = booleanOption("decrypt", options);
            algorithm = stringOption("algorithm", "PBEWithMD5AndDES", options);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof FileNameKey && this.absPath.equals(((FileNameKey) other).absPath);
        }

        @Override
        public int hashCode() {
            return this.absPath.hashCode();
        }

        public boolean isReload() {
            return reload;
        }

        public File file() {
            return file;
        }

        public boolean isDecrypt() {
            return decrypt;
        }

        public void setDecrypt(boolean decrypt) {
            this.decrypt = decrypt;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        private String stringOption(String key, String nameDefault, Map options) {
            Object result = options.get(key);
            return result != null ? result.toString() : nameDefault;
        }

        private File baseDir(Map options) {
            File baseDir = null;
            if (options.get("baseDir") != null) {
                baseDir = new File((String) options.get("baseDir"));
            } else {
                if (System.getProperty("java.security.auth.login.config") != null) {
                    baseDir = new File(System.getProperty("java.security.auth.login.config")).getParentFile();
                }
            }
            if (debug) {
                LOG.debug("Using basedir=" + baseDir.getAbsolutePath());
            }
            return baseDir;
        }

        @Override
        public String toString() {
            return "PropsFile=" + absPath;
        }

        public void setDebug(boolean debug) {
            this.debug = debug;
        }

        public boolean isDebug() {
            return debug;
        }
    }

    /**
     * For test-usage only.
     */
    public static void resetUsersAndGroupsCache() {
        staticCache.clear();
    }
}
