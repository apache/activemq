/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jmeter.visualizers;

import org.apache.jmeter.samplers.Clearable;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.visualizers.gui.AbstractVisualizer;
import org.apache.jorphan.gui.ObjectTableModel;
import org.apache.jorphan.gui.layout.VerticalLayout;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.reflect.Functor;
import org.apache.log.Logger;
import org.activemq.sampler.Producer;

import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.JScrollPane;
import javax.swing.JPanel;
import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.text.DecimalFormat;

/**
 * A tableVisualizer that can display Producer Output
 */
public class ProducerTableVisualizer extends AbstractVisualizer implements Clearable {

    private static Logger log = LoggingManager.getLoggerForClass();

    private final String[] COLUMNS = new String[]{
        JMeterUtils.getResString("table_visualizer_sample_num"),
        JMeterUtils.getResString("table_visualizer_sample"),
        JMeterUtils.getResString("table_visualizer_processed")};

    private static final long SECOND = 1000;
    private static final long INSECONDS = 60;
    private static final long MINUTE = SECOND * INSECONDS;
    private static final DecimalFormat dFormat = new DecimalFormat("#,###,###");

    private ObjectTableModel model = null;
    private JTable table = null;
    private JTextField averageField = null;
    private JTextField totalMsgsField = null;
    private JScrollPane tableScrollPanel = null;

    private double average = 0;
    private int total = 0;

    /**
     * Constructor for the TableVisualizer object.
     */
    public ProducerTableVisualizer() {

        super();

        model = new ObjectTableModel(COLUMNS,
                                     new Functor[]{new Functor("getCount"),
                                                   new Functor("getData"),
                                                   new Functor("getProcessed")},
                                     new Functor[]{null, null, null},
                                     new Class[]{Long.class, Long.class, Long.class});

        init();

    }

    /**
     * @return Label key to get from label resource
     */
    public String getLabelResource() {

        return "view_prod_results_in_table";
    }

    /**
     * Sets the average Field and total Messages sent, that would be displayed.
     */
    protected synchronized void updateTextFields() {

        averageField.setText(dFormat.format(average));
        totalMsgsField.setText(dFormat.format(total));
    }

    /**
     * @param res SampleResult from the JMeter Sampler
     */
    public void add(SampleResult res) {

        Thread timer = new Thread() {
            public void run() {
                timerLoop();
            }
        };

        timer.start();
    }

    /**
     * clear/resets the field.
     */
    public synchronized void clear() {

        log.debug("Clear called", new Exception("Debug"));
        model.clearData();
        averageField.setText("0000");
        totalMsgsField.setText("0000");
        repaint();
    }

    /**
     * Initialize the User Interface
     */
    private void init() {

        this.setLayout(new BorderLayout());

        // Main Panel
        JPanel mainPanel = new JPanel();
        Border margin = new EmptyBorder(10, 10, 5, 10);

        mainPanel.setBorder(margin);
        mainPanel.setLayout(new VerticalLayout(5, VerticalLayout.LEFT));

        // Name
        mainPanel.add(makeTitlePanel());

        // Set up the table itself
        table = new JTable(model);

        // table.getTableHeader().setReorderingAllowed(false);
        tableScrollPanel = new JScrollPane(table);
        tableScrollPanel.setViewportBorder(BorderFactory.createEmptyBorder(2, 2, 2, 2));

        // Set up footer of table which displays numerics of the graphs
        JPanel averagePanel = new JPanel();
        JLabel averageLabel =
                new JLabel(JMeterUtils.getResString("graph_results_average"));
        averageLabel.setForeground(Color.black);
        averageField = new JTextField(15);
        averageField.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
        averageField.setEditable(false);
        averageField.setForeground(Color.black);
        averageField.setBackground(getBackground());
        averagePanel.add(averageLabel);
        averagePanel.add(averageField);

        JPanel totalMsgsPanel = new JPanel();
        JLabel totalMsgsLabel =
                new JLabel(JMeterUtils.getResString("graph_results_total_msgs"));
        totalMsgsLabel.setForeground(Color.black);
        totalMsgsField = new JTextField(15);
        totalMsgsField.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
        totalMsgsField.setEditable(false);
        totalMsgsField.setForeground(Color.black);
        totalMsgsField.setBackground(getBackground());
        totalMsgsPanel.add(totalMsgsLabel);
        totalMsgsPanel.add(totalMsgsField);


        JPanel tableInfoPanel = new JPanel();
        tableInfoPanel.setLayout(new FlowLayout());
        tableInfoPanel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));

        tableInfoPanel.add(averagePanel);
        tableInfoPanel.add(totalMsgsPanel);

        // Set up the table with footer
        JPanel tablePanel = new JPanel();
        tablePanel.setLayout(new BorderLayout());
        tablePanel.add(tableScrollPanel, BorderLayout.CENTER);
        tablePanel.add(tableInfoPanel, BorderLayout.SOUTH);

        // Add the main panel and the graph
        this.add(mainPanel, BorderLayout.NORTH);
        this.add(tablePanel, BorderLayout.CENTER);
    }

    /**
     * gets the number of processed messages.
     */
    protected void timerLoop() {

        long startTime = System.currentTimeMillis();
        long difInTime = 0;
        long currTime = 0;
        long difInMins = 0;
        long difInSec = 0;
        long ramp_upInSec = 0;
        long timeInSec = 0;
        int msgCounter = 0;
        long duration = Producer.duration;
        long ramp_up = Producer.ramp_up;

        while (difInMins < duration) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            currTime = System.currentTimeMillis();
            difInTime = currTime - startTime;
            difInMins = difInTime / MINUTE;
            timeInSec = difInTime / SECOND;
            difInSec =  ((duration * INSECONDS) - timeInSec);
            ramp_upInSec = ramp_up * INSECONDS;
            long processed = Producer.resetCount();

            if (processed > 0 &&
                (difInMins >= ramp_up) &&
                ((duration - difInMins) >= ramp_up) &&
                (difInSec >= ramp_upInSec)) {

                if (timeInSec > ramp_upInSec) {
                    total += processed;
                    average = total / (timeInSec - ramp_upInSec);
                }

                // Update the footer data.
                updateTextFields();
            }
                        
            // Add data to table row.
            MessageSample newS = new MessageSample(msgCounter++, timeInSec, processed);
            model.addRow(newS);

            // check if it's time to stop the Thread.
            if (difInMins == duration){
                Producer.stopThread = true;
            }

        }
    }
}
