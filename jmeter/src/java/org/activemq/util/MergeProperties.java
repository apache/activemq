/** 
 * 
 * Copyright 2004 Protique Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/

package org.activemq.util;

/**
 *
 * Task to merge the Jmeter properties
 *
 */

import java.io.File;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.io.FileOutputStream;



/**
 * This class is used by an ant task that will merge the jmeter property files.
 *
 */
public class MergeProperties {

     //  The file to merge properties into
    protected File baseFile;
    //  The file to pull the properties from
    protected File mergeFile;

    public File getBaseFile() {
        return baseFile;
    }

    public void setBaseFile(File baseFile) {
        this.baseFile = baseFile;
    }

    public File getMergeFile() {
        return mergeFile;
    }

    public void setMergeFile(File mergeFile) {
        this.mergeFile = mergeFile;
    }

    public void mergePropertyFiles() throws FileNotFoundException, IOException {

        if (!getBaseFile().exists()) {
          throw new FileNotFoundException("Could not find file:" + getBaseFile());
        }

        if (!getMergeFile().exists()) {
          throw new FileNotFoundException("Could not find file:" + getMergeFile());
        }

        BufferedReader mergeReader = new BufferedReader(new FileReader(mergeFile));
        BufferedReader baseReader = new BufferedReader(new FileReader(baseFile));

        ArrayList baseList = new ArrayList(1024);
        ArrayList mergeList = new ArrayList(1024);

        String line = null;
        while ((line = mergeReader.readLine()) != null) {
            mergeList.add(line);
        }
        while ((line = baseReader.readLine()) != null) {
            baseList.add(line);
        }

        if(previouslyMerged(baseList, mergeList)){

            for(int i = 0; i < mergeList.size(); i++){
                int j = baseList.indexOf(mergeList.get(i));

                if(j == -1){
                    //check if key of mergeList.get(i) is in baseList
                    String mergeKey = (String) mergeList.get(i);

                    if(mergeKey.indexOf('=') > -1){
                        mergeKey = getKey(mergeKey);
                    }

                    for(int k=0; k < baseList.size(); k++){
                        String baseKey = (String) baseList.get(k);

                        if(baseKey.indexOf('=') > -1){
                            baseKey = getKey(baseKey);
                        }

                        if (mergeKey.equals(baseKey)){
                            j=k;
                        }
                    }

                    if(j > -1){
                        baseList.set(j, mergeList.get(i));
                    } else {
                        baseList.add(mergeList.get(i));
                    }
                }
            }
        } else {
            baseList.addAll((java.util.Collection) mergeList);
        }
        // write baseList to file
        FileOutputStream writer = new FileOutputStream(baseFile);
        writer.flush();
        for (int i = 0; i < baseList.size(); i++)
        {
            //System.out.println((String) baseList.get(i));
            writer.write(((String) baseList.get(i)).getBytes());
            writer.write(System.getProperty("line.separator", "\r\n").getBytes());
            writer.flush();
        }

        baseReader.close();
        mergeReader.close();
        writer.close();

    }

    public static void main(String[] args) throws Exception {
        MergeProperties mergeProperties = new MergeProperties();

        try
        {
            if (args.length < 2)
            {
                System.out.println("Usage: java OverwriteProperties c:/temp/File1.props c:/temp/File2.props");
                System.out.println("Usage: File1 will be modified, new parameters from File 2 will be added,");
                throw new Exception("Incorrect number of arguments supplied");
            }
            mergeProperties.setBaseFile(new File(args[0]));
            mergeProperties.setMergeFile(new File(args[1]));

            mergeProperties.mergePropertyFiles();

        }
        catch (FileNotFoundException ex)
        {
            System.err.println(ex.getMessage());
        }
        catch (IOException ex)
        {
            System.err.println(ex.getMessage());
        }
        catch (SecurityException ex)
        {
            System.err.println(ex.getMessage());
        }
    }

    public boolean previouslyMerged(ArrayList base, ArrayList merge) {

        return (base.contains(merge.get(0)));
    }

    public String getKey(String prop) {

        return (prop.substring(0, prop.indexOf('=')));
    }

}
