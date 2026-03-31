package eu.europeana.cloud.service.dps.storm.utils;


import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskTupleUtilityTest {

    CommonTaskTuple commonTaskTuple;
    static final String MIME_TYPE = "text/xml";

    @BeforeEach
    void init() {
        commonTaskTuple = new CommonTaskTuple();
    }


    @Test
    void parameterIsProvidedTest() {
        commonTaskTuple.addParameter(PluginParameterKeys.MIME_TYPE, MIME_TYPE);
        assertTrue(TaskTupleUtility.isProvidedAsParameter(commonTaskTuple, PluginParameterKeys.MIME_TYPE));
    }

    @Test
    void parameterIsNotProvidedTest() {
        assertFalse(TaskTupleUtility.isProvidedAsParameter(commonTaskTuple, PluginParameterKeys.MIME_TYPE));
    }


    @Test
    void getDefaultValueTest() {
        assertEquals(TaskTupleUtility.getParameterFromTuple(commonTaskTuple, PluginParameterKeys.MIME_TYPE),
                PluginParameterKeys.PLUGIN_PARAMETERS.get(PluginParameterKeys.MIME_TYPE));
    }


    @Test
    void getProvidedValueTest() {
        commonTaskTuple.addParameter(PluginParameterKeys.MIME_TYPE, MIME_TYPE);
        assertEquals(TaskTupleUtility.getParameterFromTuple(commonTaskTuple, PluginParameterKeys.MIME_TYPE), MIME_TYPE);

    }


}
