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
package org.activemq.sampler.control.gui;

import org.apache.jmeter.config.gui.LoginConfigGui;
import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.util.JMeterUtils;
import org.activemq.sampler.config.gui.ProducerConfigGui;
import org.activemq.sampler.Producer;

import javax.swing.BorderFactory;

import java.awt.BorderLayout;

/**
 * Form in JMeter to enter default values for generating the sampler set.
 */
public class ProducerSamplerGui extends AbstractSamplerGui {

    //private LoginConfigGui loginPanel;
    private ProducerConfigGui TcpDefaultPanel;

    /**
     * Constructor for the ProducerSamplerGui object
     */
    public ProducerSamplerGui() {

        init();
    }

    /**
     * Method for configuring the ProducerSamplerGui
     *
     * @param element
     */
    public void configure(TestElement element) {

        super.configure(element);
        //loginPanel.configure(element);
        TcpDefaultPanel.configure(element);
    }

    /**
     * Method for creating test elements
     *
     * @return returns a sampler
     */
    public TestElement createTestElement() {

        Producer sampler = new Producer();
        modifyTestElement(sampler);
        return sampler;
    }

    /**
     * Method to modify test elements
     *
     * @param sampler
     */
    public void modifyTestElement(TestElement sampler) {

        sampler.clear();
        ((Producer) sampler).addTestElement(TcpDefaultPanel.createTestElement());
        //((Producer) sampler).addTestElement(loginPanel.createTestElement());
        this.configureTestElement(sampler);
    }

    /**
     * Getter method for the LabelResource property.
     *
     * @return String constant "producer_sample_title"
     */
    public String getLabelResource() {

        return "producer_sample_title";
    }

    /**
     * Method to initialize ProducerSamplerGui. Sets up the layout of the GUI.
     */
    private void init() {

        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());

        add(makeTitlePanel(), BorderLayout.NORTH);

        VerticalPanel mainPanel = new VerticalPanel();

        TcpDefaultPanel = new ProducerConfigGui(false);
        mainPanel.add(TcpDefaultPanel);

        //loginPanel = new LoginConfigGui(false);
        //loginPanel.setBorder(BorderFactory.createTitledBorder(JMeterUtils.getResString("login_config")));
        //mainPanel.add(loginPanel);

        add(mainPanel, BorderLayout.CENTER);
    }
}
