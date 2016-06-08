/**
 * Copyright 2016 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FI-WARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */
package com.telefonica.iot.cygnus.sinks;

import com.telefonica.iot.cygnus.sinks.Enums.DataModel;
import static com.telefonica.iot.cygnus.utils.CommonUtilsForTests.getTestTraceHead;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.flume.Context;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author frb
 */
public class NGSISinkTest {
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    /**
     * This class is used to test once and only once the common functionality shared by all the real extending sinks.
     */
    private class NGSISinkImpl extends NGSISink {

        @Override
        void persistBatch(NGSIBatch batch) throws Exception {
            throw new UnsupportedOperationException("Not supported yet.");
        } // persistBatch
        
    } // NGSISinkImpl
    
    /**
     * Constructor.
     */
    public NGSISinkTest() {
        LogManager.getRootLogger().setLevel(Level.FATAL);
    } // NGSISinkTest
    
    /**
     * [CygnusSink.start] -------- The sink starts properly.
     */
    @Test
    public void testStart() {
        System.out.println(getTestTraceHead("[NGSISink.start]") + "-------- The sink starts properly");
        NGSISinkImpl sink = new NGSISinkImpl();
        sink.configure(createContext(null, null, null, null, null, null, null)); // default configuration
        sink.setChannel(new MemoryChannel());
        sink.start();
        LifecycleState state = sink.getLifecycleState();
        
        try {
            assertEquals(LifecycleState.START, state);
            System.out.println(getTestTraceHead("[NGSISink.start]")
                    + "-  OK  - The sink started properly, the lifecycle state is '" + state.toString() + "'");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.start]")
                    + "- FAIL - The sink did not start properly, the lifecycle state is '" + state.toString() + "'");
        } // try catch
    } // testStart
    
    /**
     * [CygnusSink.configure] -------- When not configured, the default values are used for non mandatory
     * parameters.
     */
    @Test
    public void testConfigureNotMandatoryParameters() {
        System.out.println(getTestTraceHead("[NGSISink.configure]")
                + "-------- When not configured, the default values are used for non mandatory parameters");
        NGSISinkImpl sink = new NGSISinkImpl();
        sink.configure(createContext(null, null, null, null, null, null, null)); // default configuration
        
        try {
            assertEquals(1, sink.getBatchSize());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - The default configuration value for 'batch_size' is '1'");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - The default configuration value for 'batch_size' is '" + sink.getBatchSize() + "'");
            throw e;
        } // try catch
        
        try {
            assertEquals(30, sink.getBatchTimeout());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - The default configuration value for 'batch_timeout' is '30'");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - The default configuration value for 'batch_timeout' is '"
                    + sink.getBatchTimeout() + "'");
            throw e;
        } // try catch
        
        try {
            assertEquals(10, sink.getBatchTTL());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - The default configuration value for 'batch_ttl' is '10'");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - The default configuration value for 'batch_ttl' is '" + sink.getBatchTTL() + "'");
            throw e;
        } // try catch
        
        try {
            assertEquals(DataModel.DMBYENTITY, sink.getDataModel());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - The default configuration value for 'data_model' is 'dm-by-entity'");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - The default configuration value for "
                    + "'data_model' is '" + sink.getDataModel() + "'");
            throw e;
        } // try catch
        
        try {
            assertEquals(false, sink.getEnableGrouping());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - The default configuration value for 'enable_grouping' is 'false'");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - The default configuration value for "
                    + "'enable_grouping' is '" + sink.getEnableGrouping() + "'");
            throw e;
        } // try catch
        
        try {
            assertEquals(false, sink.getEnableLowerCase());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - The default configuration value for 'enable_lowercase' is 'false'");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - The default configuration value for "
                    + "'enable_lowercase' is '" + sink.getEnableLowerCase() + "'");
            throw e;
        } // try catch
    } // testConfigureNotMandatoryParameters
    
    /**
     * [CygnusSink.configure] -------- The configuration becomes invalid upon out-of-the-limits configured values
     * for parameters having a discrete set of accepted values, or numerical values having upper or lower limits.
     */
    @Test
    public void testConfigureInvalidConfiguration() {
        System.out.println(getTestTraceHead("[NGSISink.configure]")
                + "-------- The configuration becomes invalid upon out-of-the-limits configured values for parameters "
                + "having a discrete set of accepted values, or numerical values having upper or lower limits");
        NGSISinkImpl sink = new NGSISinkImpl();
        String configuredBatchSize = "0";
        sink.configure(createContext(null, configuredBatchSize, null, null, null, null, null));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - A wrong configuration 'batch_size='"
                    + configuredBatchSize + "' has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - A wrong configuration 'batch_size='"
                    + configuredBatchSize + "' has not been detected");
            throw e;
        } // try catch
        
        sink = new NGSISinkImpl();
        String configuredBatchTimeout = "0";
        sink.configure(createContext(null, null, configuredBatchTimeout, null, null, null, null));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - A wrong configuration 'batch_timeout='"
                    + configuredBatchTimeout + "' has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - A wrong configuration 'batch_timeout='"
                    + configuredBatchTimeout + "' has not been detected");
            throw e;
        } // try catch
        
        sink = new NGSISinkImpl();
        String configuredBatchTTL = "-2";
        sink.configure(createContext(null, null, null, configuredBatchTTL, null, null, null));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - A wrong configuration 'batch_ttl='" + configuredBatchTTL + "' has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - A wrong configuration 'batch_ttl='" + configuredBatchTTL + "' has not been detected");
            throw e;
        } // try catch
        
        sink = new NGSISinkImpl();
        String dataModel = "dm-by-other";
        sink.configure(createContext(null, null, null, null, dataModel, null, null));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - A wrong configuration 'data_model='" + dataModel + "' has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - A wrong configuration 'data_model='" + dataModel + "' has not been detected");
            throw e;
        } // try catch
        
        sink = new NGSISinkImpl();
        String configuredEnableGrouping = "falso";
        sink.configure(createContext(null, null, null, null, null, configuredEnableGrouping, null));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - A wrong configuration 'enable_grouping='"
                    + configuredEnableGrouping + "' has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - A wrong configuration 'enable_grouping='"
                    + configuredEnableGrouping + "' has not been detected");
            throw e;
        } // try catch
        
        sink = new NGSISinkImpl();
        String configuredEnableLowercase = "verdadero";
        sink.configure(createContext(null, null, null, null, null, null, configuredEnableLowercase));
        
        try {
            assertTrue(sink.getInvalidConfiguration());
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - A wrong configuration 'enable_lowercase='"
                    + configuredEnableLowercase + "' has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - A wrong configuration 'enable_lowercase='"
                    + configuredEnableLowercase + "' has not been detected");
            throw e;
        } // try catch
    } // testConfigureInvalidConfiguration
    
    /**
     * [CygnusSink.configure] -------- When configured, empty attribute mappings are detected.
     */
    @Test
    public void testConfigureEmptyAttributeMappings() {
        System.out.println(getTestTraceHead("[NGSISink.configure]")
                + "-------- When configured, empty attribute mappings are detected");
        File file;
        
        try {
            file = folder.newFile("attribute_mappings.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"attribute_mappings\":[]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        NGSISinkImpl sink = new NGSISinkImpl();
        String attributeMappings = file.getAbsolutePath();
        String batchSize = null; // defaulting
        String batchTimeout = null; // defaulting
        String batchTTL = null; // defaulting
        String dataModel = null; // defaulting
        String enableGrouping = null; // defaulting
        String enableLowercase = null; // defaulting
        sink.configure(createContext(attributeMappings, batchSize, batchTimeout, batchTTL, dataModel, enableGrouping,
                enableLowercase));
        
        try {
            assertTrue(sink.attributeMappings == null);
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - Empty attribute mappings have been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - Empty attrbiute mappings have not been detected");
            throw e;
        } // try catch
    } // testConfigureEmptyAttributeMappings
    
    /**
     * [CygnusSink.configure] -------- When configured, invalid attribute mappings are detected.
     */
    @Test
    public void testConfigureInvalidAttributeMappings() {
        System.out.println(getTestTraceHead("[NGSISink.configure]")
                + "-------- When configured, invalid attribute mappings are detected");
        File file;
        
        try {
            file = folder.newFile("attribute_mappings.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"attribute_mappings\":[{\"entity_id\":\"car1\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        NGSISinkImpl sink = new NGSISinkImpl();
        String attributeMappings = file.getAbsolutePath();
        String batchSize = null; // defaulting
        String batchTimeout = null; // defaulting
        String batchTTL = null; // defaulting
        String dataModel = null; // defaulting
        String enableGrouping = null; // defaulting
        String enableLowercase = null; // defaulting
        sink.configure(createContext(attributeMappings, batchSize, batchTimeout, batchTTL, dataModel, enableGrouping,
                enableLowercase));
        
        try {
            assertTrue(sink.attributeMappings == null);
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - An invalid attribute mapping has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - An invalid attribute mapping has not been detected");
            throw e;
        } // try catch
    } // testConfigureIvalidAttributeMappings
    
    /**
     * [CygnusSink.configure] -------- When attribute mappings is not configured, the configuration is valid and no
     * default value is used.
     */
    @Test
    public void testConfigureNullAttributeMappings() {
        System.out.println(getTestTraceHead("[NGSISink.configure]")
                + "-------- When attribute mappings is not configured, the configuration is valid and no default "
                + "value is used");
        NGSISinkImpl sink = new NGSISinkImpl();
        String attributeMappings = null; // not configured
        String batchSize = null; // defaulting
        String batchTimeout = null; // defaulting
        String batchTTL = null; // defaulting
        String dataModel = null; // defaulting
        String enableGrouping = null; // defaulting
        String enableLowercase = null; // defaulting
        sink.configure(createContext(attributeMappings, batchSize, batchTimeout, batchTTL, dataModel, enableGrouping,
                enableLowercase));
        
        try {
            assertTrue(!sink.invalidConfiguration && sink.attributeMappings == null);
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "-  OK  - Attribute mappings have not been configured and the configuration is still valid");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSISink.configure]")
                    + "- FAIL - Attribute mappings have not been configured and the configuration is not valid");
            throw e;
        } // try catch
    } // testConfigureNullAttributeMappings
    
    private Context createContext(String attributeMappings, String batchSize, String batchTimeout, String batchTTL,
            String dataModel, String enableGrouping, String enableLowercase) {
        Context context = new Context();
        context.put("attribute_mappings", attributeMappings);
        context.put("batch_size", batchSize);
        context.put("batch_timeout", batchTimeout);
        context.put("batch_ttl", batchTTL);
        context.put("data_model", dataModel);
        context.put("enable_grouping", enableGrouping);
        context.put("enable_lowercase", enableLowercase);
        return context;
    } // createContext
    
} // NGSISinkTest
