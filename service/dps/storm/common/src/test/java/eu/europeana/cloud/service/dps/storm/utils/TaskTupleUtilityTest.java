package eu.europeana.cloud.service.dps.storm.utils;


import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TaskTupleUtilityTest {

    StormTaskTuple stormTaskTuple;
    static final String MIME_TYPE = "text/xml";

    @BeforeEach
    void init() {
        stormTaskTuple = new StormTaskTuple();
    }


    @Test
    void parameterIsProvidedTest() {
        stormTaskTuple.addParameter(PluginParameterKeys.MIME_TYPE, MIME_TYPE);
        assertTrue(TaskTupleUtility.isProvidedAsParameter(stormTaskTuple, PluginParameterKeys.MIME_TYPE));
    }

    @Test
    void parameterIsNotProvidedTest() {
        assertFalse(TaskTupleUtility.isProvidedAsParameter(stormTaskTuple, PluginParameterKeys.MIME_TYPE));
    }


    @Test
    void getDefaultValueTest() {
        assertEquals(TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.MIME_TYPE),
                PluginParameterKeys.PLUGIN_PARAMETERS.get(PluginParameterKeys.MIME_TYPE));
    }


    @Test
    void getProvidedValueTest() {
        stormTaskTuple.addParameter(PluginParameterKeys.MIME_TYPE, MIME_TYPE);
        assertEquals(TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.MIME_TYPE), MIME_TYPE);

    }


}
