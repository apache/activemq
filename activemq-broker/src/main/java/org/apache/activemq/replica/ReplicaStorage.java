package org.apache.activemq.replica;

import org.apache.activemq.util.IOHelper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ReplicaStorage {

    private final String storageName;

    private File storage;
    private File storageTmp;

    public ReplicaStorage(String storageName) {
        this.storageName = storageName;
    }

    public void initialize(File directory) throws IOException {
        IOHelper.mkdirs(directory);

        storage = new File(directory, storageName);
        storageTmp = new File(directory, storageName + "_tmp");
    }

    public String read() throws IOException {
        restoreIfNeeded();

        if (!storage.exists()) {
            return null;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(storage))) {
            String line = reader.readLine();
            if (line == null || line.isBlank()) {
                return null;
            }
            return line;
        }
    }

    public void write(String line) throws IOException {
        restoreIfNeeded();

        try (FileWriter fileWriter = new FileWriter(storageTmp)) {
            fileWriter.write(line);
            fileWriter.flush();
        }

        copyTmpToMain();
    }

    private void restoreIfNeeded() throws IOException {
        if (!storageTmp.exists()) {
            return;
        }
        copyTmpToMain();
    }

    private void copyTmpToMain() throws IOException {
        if (storage.exists()) {
            if (!storage.delete()) {
                throw new IOException("Could not delete main storage: " + storageName);
            }
        }
        if (!storageTmp.renameTo(storage)) {
            throw new IOException("Could not move temp storage to main storage: " + storageName);
        }
    }
}
