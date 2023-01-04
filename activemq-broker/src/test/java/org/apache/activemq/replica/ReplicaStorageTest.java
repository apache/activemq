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
package org.apache.activemq.replica;

import org.apache.activemq.util.IOHelper;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import static org.assertj.core.api.Assertions.assertThat;

public class ReplicaStorageTest {

    private final String storageName = getClass().getName();

    private final File brokerDataDirectory = new File(IOHelper.getDefaultDataDirectory());
    private final File storage = new File(brokerDataDirectory, storageName);
    private final File storageTmp = new File(brokerDataDirectory, storageName + "_tmp");

    private final ReplicaStorage replicaStorage = new ReplicaStorage(storageName);

    @Before
    public void setUp() throws Exception {
        replicaStorage.initialize(brokerDataDirectory);

        if (storage.exists()) {
            assertThat(storage.delete()).isTrue();
        }
        if (storageTmp.exists()) {
            assertThat(storageTmp.delete()).isTrue();
        }
    }

    @Test
    public void readTest() throws Exception {
        String testString = getMethodName();
        try (FileWriter writer = new FileWriter(storage)) {
            writer.write(testString);
            writer.flush();
        }

        assertThat(replicaStorage.read()).isEqualTo(testString);
        assertThat(storageTmp.exists()).isFalse();
    }

    @Test
    public void readWhenTmpStorageIsPresentTest() throws Exception {
        String testString = getMethodName();
        try (FileWriter writer = new FileWriter(storageTmp)) {
            writer.write(testString);
            writer.flush();
        }

        assertThat(replicaStorage.read()).isEqualTo(testString);
        assertThat(storageTmp.exists()).isFalse();
    }

    @Test
    public void readWhenTmpAndMainStoragesArePresentTest() throws Exception {
        String testString = getMethodName();
        try (FileWriter writer = new FileWriter(storage)) {
            writer.write("test");
            writer.flush();
        }
        try (FileWriter writer = new FileWriter(storageTmp)) {
            writer.write(testString);
            writer.flush();
        }

        assertThat(replicaStorage.read()).isEqualTo(testString);
        assertThat(storageTmp.exists()).isFalse();
    }

    @Test
    public void writeTest() throws Exception {
        String testString = getMethodName();

        replicaStorage.write(testString);

        try (BufferedReader reader = new BufferedReader(new FileReader(storage))) {
            assertThat(reader.readLine()).isEqualTo(testString);
        }
        assertThat(storageTmp.exists()).isFalse();
    }

    @Test
    public void writeWhenTmpStorageIsPresentTest() throws Exception {
        String testString = getMethodName();

        try (FileWriter writer = new FileWriter(storageTmp)) {
            writer.write("test");
            writer.flush();
        }

        replicaStorage.write(testString);

        try (BufferedReader reader = new BufferedReader(new FileReader(storage))) {
            assertThat(reader.readLine()).isEqualTo(testString);
        }
        assertThat(storageTmp.exists()).isFalse();
    }

    private String getMethodName() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

        return stackTrace[2].getMethodName();
    }
}
