package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester.Harvester;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper.WiremockHelper;
import org.apache.storm.task.OutputCollector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import javax.xml.xpath.XPathExpression;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link RecordHarvestingBolt}
 */

@RunWith(MockitoJUnitRunner.class)
public class RecordHarvestingBoltTest extends WiremockHelper {
    @Mock
    private OutputCollector outputCollector;

    @Mock
    private Harvester harvester;

    @InjectMocks
    private RecordHarvestingBolt recordHarvestingBolt = new RecordHarvestingBolt();

    @Test
    public void harvestingForAllParametersSpecified() throws IOException, HarvesterException {
        //given

        InputStream fileContentAsStream = getFileContentAsStream("/sampleEDMRecord.xml");
        when(harvester.harvestRecord(anyString(), anyString(), anyString(),any(XPathExpression.class))).thenReturn(fileContentAsStream);
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifySuccessfulEmit();
        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
    }


    @Test
    public void shouldHarvestRecordInEDMAndExtractIdentifiers() throws IOException, HarvesterException {

        //given
        InputStream fileContentAsStream = getFileContentAsStream("/sampleEDMRecord.xml");
        when(harvester.harvestRecord(anyString(), anyString(), anyString(),any(XPathExpression.class))).thenReturn(fileContentAsStream);
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifySuccessfulEmit();

        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
        assertEquals("http://more.locloud.eu/object/DCU/24927017", spiedTask.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
        assertEquals("/2020739_Ag_EU_CARARE_2Cultur/object_DCU_24927017", spiedTask.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
    }

    @Test
    public void shouldHarvestRecordInEDMAndNotUseHeaderIdentifierIfParameterIsDifferentThanTrue() throws IOException, HarvesterException {

        //given
        InputStream fileContentAsStream = getFileContentAsStream("/sampleEDMRecord.xml");
        when(harvester.harvestRecord(anyString(), anyString(), anyString(),any(XPathExpression.class))).thenReturn(fileContentAsStream);

        StormTaskTuple task = taskWithGivenValueOfUseHeaderIdentifiersParameter("blablaba");
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifySuccessfulEmit();

        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
        assertEquals("http://more.locloud.eu/object/DCU/24927017",  spiedTask.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
        assertEquals("/2020739_Ag_EU_CARARE_2Cultur/object_DCU_24927017", spiedTask.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
    }


    @Test
    public void shouldHarvestRecordInEDMAndUseHeaderIdentifierIfSpecifiedInTaskParameters() throws IOException, HarvesterException {

        //given
        InputStream fileContentAsStream = getFileContentAsStream("/sampleEDMRecord.xml");
        when(harvester.harvestRecord(anyString(), anyString(), anyString(),any(XPathExpression.class))).thenReturn(fileContentAsStream);

        StormTaskTuple task = taskWithGivenValueOfUseHeaderIdentifiersParameter("true");
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifySuccessfulEmit();

        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
        assertEquals(null,  spiedTask.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
        assertEquals("http://data.europeana.eu/item/2064203/o_aj_kk_tei_3", spiedTask.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
    }


    @Test
    public void shouldHarvestRecordInEDMAndUseHeaderIdentifierAndTrimItIfSpecifiedInTaskParameters() throws IOException, HarvesterException {

        //given
        InputStream fileContentAsStream = getFileContentAsStream("/sampleEDMRecord.xml");
        when(harvester.harvestRecord(anyString(), anyString(), anyString(),any(XPathExpression.class))).thenReturn(fileContentAsStream);

        StormTaskTuple task = taskWithGivenValueOfUseHeaderIdentifiersAndTrimmingPrefix("true");
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifySuccessfulEmit();

        verify(spiedTask).setFileData(Mockito.any(InputStream.class));
        assertEquals(null,  spiedTask.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
        assertEquals("/item/2064203/o_aj_kk_tei_3", spiedTask.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
    }


    @Test
    public void shouldEmitErrorOnHarvestingExceptionWhenCannotExctractEuropeanaIdFromEDM() throws IOException, HarvesterException {

        //given
        InputStream fileContentAsStream = getFileContentAsStream("/corruptedEDMRecord.xml");
        when(harvester.harvestRecord(anyString(), anyString(), anyString(),any(XPathExpression.class))).thenReturn(fileContentAsStream);
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifyErrorEmit();
    }

    @Test
    public void shouldEmitErrorOnHarvestingException() throws IOException,
            HarvesterException {
        //given
        when(harvester.harvestRecord(anyString(), anyString(), anyString(),any(XPathExpression.class))).thenThrow(new
                HarvesterException("Some!"));
        StormTaskTuple task = taskWithAllNeededParameters();
        StormTaskTuple spiedTask = spy(task);

        //when
        recordHarvestingBolt.execute(spiedTask);

        //then
        verifyErrorEmit();
    }

    @Test
    public void harvestingForEmptyUrl() {
        //given
        StormTaskTuple task = taskWithoutResourceUrl();

        //when
        recordHarvestingBolt.execute(task);

        //then
        verifyErrorEmit();
    }

    @Test
    public void harvestingForEmptyRecordId() {
        //given
        StormTaskTuple task = taskWithoutRecordId();

        //when
        recordHarvestingBolt.execute(task);

        //then
        verifyErrorEmit();
    }

    @Test
    public void harvestForEmptyPrefix() {
        //given
        StormTaskTuple task = taskWithoutPrefix();

        //when
        recordHarvestingBolt.execute(task);

        //then
        verifyErrorEmit();
    }

    private StormTaskTuple taskWithGivenValueOfUseHeaderIdentifiersAndTrimmingPrefix(String paramValue){
        StormTaskTuple stormTaskTuple = taskWithGivenValueOfUseHeaderIdentifiersParameter(paramValue);
        stormTaskTuple.addParameter(PluginParameterKeys.MIGRATION_IDENTIFIER_PREFIX, "http://data.europeana.eu");
        return stormTaskTuple;
    }

    private StormTaskTuple taskWithGivenValueOfUseHeaderIdentifiersParameter(String paramValue){
        StormTaskTuple stormTaskTuple = taskWithAllNeededParameters();
        stormTaskTuple.addParameter(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS, paramValue);
        stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, "http://data.europeana.eu/item/2064203/o_aj_kk_tei_3");
        return stormTaskTuple;
    }

    private StormTaskTuple taskWithAllNeededParameters() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
        task.setSourceDetails(details);
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
        task.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, "oaiIdentifier");
        task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
        task.addParameter(PluginParameterKeys.METIS_DATASET_ID, "2020739_Ag_EU_CARARE_2Culture");
        return task;
    }

    private StormTaskTuple taskWithoutResourceUrl() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails("schema");
        task.setSourceDetails(details);
        return task;
    }



    private StormTaskTuple taskWithoutRecordId() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
        task.setSourceDetails(details);
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
        task.addParameter(PluginParameterKeys.SCHEMA_NAME, "schema");
        return task;
    }

    private StormTaskTuple taskWithoutPrefix() {
        StormTaskTuple task = new StormTaskTuple();
        OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
        task.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, "urlToOAIEndpoint");
        task.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, "oaiIdentifier");
        task.setSourceDetails(details);
        return task;
    }

     /**

     * Checks if emit to standard stream occured
     */
    private void verifySuccessfulEmit() {
        verify(outputCollector, times(1)).emit(Mockito.anyList());
        verify(outputCollector, times(0)).emit(eq("NotificationStream"),Mockito.anyList());
    }

    /**
     * Checks if emit to error stream occured
     */
    private void verifyErrorEmit() {

        verify(outputCollector, times(1)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(outputCollector, times(0)).emit(Mockito.anyList());
    }

}
