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

package org.activemq.util.ant;

/**
 *
 *<code>MergePropertiesTask</code> is the task definition for an Ant
 * interface to the <code>MergeProperties</code>.
 */

import java.io.File;

import org.apache.tools.ant.Task;
import org.apache.tools.ant.BuildException;

import org.activemq.util.MergeProperties;


public class MergePropertiesTask extends Task{

    /** File to merge properties into */
    private File mergeBaseProperties;

    /** File to merge properties from */
    private File mergeProperties;

    /** Fail on error flag */
    private boolean failonerror = true;

    /**
     *  Sets the File to merge properties into
     *
     * @param mergeBaseProperties
     *               File to merge properties into
     */

    public void setMergeBaseProperties(File mergeBaseProperties)
    {
        this.mergeBaseProperties = mergeBaseProperties;
    }

    /**
     *  Sets the File to merge properties from
     *
     * @param  mergeProperties  File to merge properties from
     */

    public void setMergeProperties(File mergeProperties)
    {
        this.mergeProperties = mergeProperties;
    }

     public void setFailOnError(boolean failonerror)
     {
         this.failonerror = failonerror;
     }

    /**
     *  Gets the File to merge properties into
     *
     * @return File to merge properties into
     */
    public File getMergeBaseProperties()
    {

        return mergeBaseProperties;
    }

    /**
     *  Gets the File to merge properties from
     *
     * @return File to merge properties from
     */
    public File getMergeProperties()
    {

        return mergeProperties;
    }

    /**
     * Load the step and then execute it
     *
     * @exception BuildException
     *                   Description of the Exception
     */
    public void execute() throws BuildException
    {

        try
        {
            MergeProperties mergeProperties = new MergeProperties();
            mergeProperties.setBaseFile(getMergeBaseProperties());
            mergeProperties.setMergeFile(getMergeProperties());

            mergeProperties.mergePropertyFiles();
        }
        catch (Exception e)
        {
            if (!this.failonerror)
            {
                log(e.toString());
            }
            else
            {
                throw new BuildException(e.toString());
            }
        }
    }
}