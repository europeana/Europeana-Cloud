package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

/**
 * Created by Tarek on 11/13/2015.
 */
public class TaskTupleUtilityTest {
    TaskTupleUtility taskTupleUtility;
    StormTaskTuple stormTaskTuple;

    @Before
    public void init() {
        taskTupleUtility = new TaskTupleUtility();
        stormTaskTuple = new StormTaskTuple();
    }


    @Test
    public void parameterIsProvided() {
        stormTaskTuple.addParameter(PluginParameterKeys.MIME_TYPE, "text/xml");
        assertTrue(taskTupleUtility.isProvidedAsParameter(stormTaskTuple, PluginParameterKeys.MIME_TYPE));
    }

    @Test
    public void parameterIsNotProvided() {
        assertFalse(taskTupleUtility.isProvidedAsParameter(stormTaskTuple, PluginParameterKeys.MIME_TYPE));
    }


    @Test
    public void getDefaultValue() {
        assertEquals(taskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.MIME_TYPE),PluginParameterKeys.PLUGIN_PARAMETERS.get(PluginParameterKeys.MIME_TYPE));
    }


    @Test
    public void getProvidedValue() {
        assertFalse(taskTupleUtility.isProvidedAsParameter(stormTaskTuple, PluginParameterKeys.MIME_TYPE));
    }



}
