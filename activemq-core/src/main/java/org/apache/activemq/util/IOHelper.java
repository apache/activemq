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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @version $Revision$
 */
public final class IOHelper {
    protected static final int MAX_DIR_NAME_LENGTH;
    protected static final int MAX_FILE_NAME_LENGTH;
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private IOHelper() {
    }

    public static String getDefaultDataDirectory() {
        return getDefaultDirectoryPrefix() + "activemq-data";
    }

    public static String getDefaultStoreDirectory() {
        return getDefaultDirectoryPrefix() + "amqstore";
    }

    /**
     * Allows a system property to be used to overload the default data
     * directory which can be useful for forcing the test cases to use a target/
     * prefix
     */
    public static String getDefaultDirectoryPrefix() {
        try {
            return System.getProperty("org.apache.activemq.default.directory.prefix", "");
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Converts any string into a string that is safe to use as a file name.
     * The result will only include ascii characters and numbers, and the "-","_", and "." characters.
     *
     * @param name
     * @return
     */
    public static String toFileSystemDirectorySafeName(String name) {
        return toFileSystemSafeName(name, true, MAX_DIR_NAME_LENGTH);
    }
    
    public static String toFileSystemSafeName(String name) {
        return toFileSystemSafeName(name, false, MAX_FILE_NAME_LENGTH);
    }
    
    /**
     * Converts any string into a string that is safe to use as a file name.
     * The result will only include ascii characters and numbers, and the "-","_", and "." characters.
     *
     * @param name
     * @param dirSeparators 
     * @param maxFileLength 
     * @return
     */
    public static String toFileSystemSafeName(String name,boolean dirSeparators,int maxFileLength) {
        int size = name.length();
        StringBuffer rc = new StringBuffer(size * 2);
        for (int i = 0; i < size; i++) {
            char c = name.charAt(i);
            boolean valid = c >= 'a' && c <= 'z';
            valid = valid || (c >= 'A' && c <= 'Z');
            valid = valid || (c >= '0' && c <= '9');
            valid = valid || (c == '_') || (c == '-') || (c == '.') || (c=='#')
                    ||(dirSeparators && ( (c == '/') || (c == '\\')));

            if (valid) {
                rc.append(c);
            } else {
                // Encode the character using hex notation
                rc.append('#');
                rc.append(HexSupport.toHexFromInt(c, true));
            }
        }
        String result = rc.toString();
        if (result.length() > maxFileLength) {
            result = result.substring(result.length()-maxFileLength,result.length());
        }
        return result;
    }
    
    public static boolean deleteFile(File fileToDelete) {
        if (fileToDelete == null || !fileToDelete.exists()) {
            return true;
        }
        boolean result = deleteChildren(fileToDelete);
        result &= fileToDelete.delete();
        return result;
    }
    
    public static boolean deleteChildren(File parent) {
        if (parent == null || !parent.exists()) {
            return false;
        }
        boolean result = true;
        if (parent.isDirectory()) {
            File[] files = parent.listFiles();
            if (files == null) {
                result = false;
            } else {
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    if (file.getName().equals(".")
                            || file.getName().equals("..")) {
                        continue;
                    }
                    if (file.isDirectory()) {
                        result &= deleteFile(file);
                    } else {
                        result &= file.delete();
                    }
                }
            }
        }
       
        return result;
    }
    
    
    public static void moveFile(File src, File targetDirectory) throws IOException {
        if (!src.renameTo(new File(targetDirectory, src.getName()))) {
            throw new IOException("Failed to move " + src + " to " + targetDirectory);
        }
    }
    
    public static void copyFile(File src, File dest) throws IOException {
        copyFile(src,dest,null);
    }
    
    public static void copyFile(File src, File dest,FilenameFilter filter) throws IOException {
        if (src.getCanonicalPath().equals(dest.getCanonicalPath()) == false) {
            if (src.isDirectory()) {
                if (dest.isDirectory()) {
                    mkdirs(dest);
                    List<File> list = getFiles(src,filter);
                    for (File f : list) {
                        if (f.isFile()) {
                            File target = new File(getCopyParent(src, dest, f), f.getName());
                            copySingleFile(f, target);
                        }
                    }

                }
            } else if (dest.isDirectory()) {
                mkdirs(dest);
                File target = new File(dest, src.getName());
                copySingleFile(src, target);
            } else {
                copySingleFile(src, dest);
            }
        }
    }
    
    static File getCopyParent(File from, File to, File src) {
        File result = null;
        File parent = src.getParentFile();
        String fromPath = from.getAbsolutePath();
        if (parent.getAbsolutePath().equals(fromPath)) {
            //one level down
            result = to;
        }else {
            String parentPath = parent.getAbsolutePath();
            String path = parentPath.substring(fromPath.length());
            result = new File(to.getAbsolutePath()+File.separator+path);
        }
        return result;
    }
    
    static List<File> getFiles(File dir,FilenameFilter filter){
        List<File> result = new ArrayList<File>();
        getFiles(dir,result,filter);
        return result;
    }
    
    static void getFiles(File dir,List<File> list,FilenameFilter filter) {
        if (!list.contains(dir)) {
            list.add(dir);
            String[] fileNames=dir.list(filter);
            for (int i =0; i < fileNames.length;i++) {
                File f = new File(dir,fileNames[i]);
                if (f.isFile()) {
                    list.add(f);
                }else {
                    getFiles(dir,list,filter);
                }
            }
        }
    }
    
    
    public static void copySingleFile(File src, File dest) throws IOException {
        FileInputStream fileSrc = new FileInputStream(src);
        FileOutputStream fileDest = new FileOutputStream(dest);
        copyInputStream(fileSrc, fileDest);
    }
    
    public static void copyInputStream(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int len = in.read(buffer);
        while (len >= 0) {
            out.write(buffer, 0, len);
            len = in.read(buffer);
        }
        in.close();
        out.close();
    }
    
    static {
        MAX_DIR_NAME_LENGTH = Integer.valueOf(System.getProperty("MaximumDirNameLength","200")).intValue();  
        MAX_FILE_NAME_LENGTH = Integer.valueOf(System.getProperty("MaximumFileNameLength","64")).intValue();             
    }

    
    public static void mkdirs(File dir) throws IOException {
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IOException("Failed to create directory '" + dir +"', regular file already existed with that name");
            }
            
        } else {
            if (!dir.mkdirs()) {
                throw new IOException("Failed to create directory '" + dir+"'");
            }
        }
    }
}
