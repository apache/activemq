/**
 *
 * Copyright 2004 The Apache Software Foundation
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
 */

package org.activemq.sampler.config;

import org.apache.jmeter.config.ConfigTestElement;
import org.activemq.sampler.Sampler;

import java.io.Serializable;

/**
 * Producer configuration bean.
 */
public class SamplerConfig extends ConfigTestElement implements Serializable {

    /**
     * Default constructor.
     */
    public SamplerConfig() {
    }

    /**
     * Sets the producer sampler filename property.
     *
     * @param newFilename
     */
    public void setFilename(String newFilename) {
        this.setProperty(Sampler.FILENAME, newFilename);
    }

    /**
     * Returns the producer sampler filename property.
     *
     * @return  producer sampler filename
     */
    public String getFilename() {
        return getPropertyAsString(Sampler.FILENAME);
    }

    /**
     * Returns the producer sampler url property.
     *
     * @return url
     */
    public String getLabel() {
        return (this.getUrl());
    }

    /**
         * Sets the producer sampler url property.
         *
         * @param newUrl
         */
        public void setUrl(String newUrl) {
            this.setProperty(Sampler.URL, newUrl);
        }

        /**
         * Returns the producer sampler url property.
         *
         * @return producer url
         */
        public String getUrl() {
            return getPropertyAsString(Sampler.URL);
        }

        

}
