package eu.europeana.cloud.service.dps.services.task.postprocessors;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;

@RunWith(MockitoJUnitRunner.class)
public class HarvestingPostProcessorTest {

    private static final long TASK_ID = 1000;
    private static final String METIS_DATASET_ID = "111";
    private static final String PROVIDER_ID = "prov";
    private static final String DATASET_ID = "datasetId";
    private static final String OUTPUT_DATA_SETS = "http://localhost:8080/mcs/data-providers/prov/data-sets/datasetId";
    private static final String RECORD_ID1 = "R1";
    private static final String RECORD_ID2 = "R2";
    private static final String CLOUD_ID1 = "a1";
    private static final String CLOUD_ID2 = "b2";
    private static final String REPRESENTATION_NAME = "repr";
    private static final String VERSION = "v1";
    private static final String RECORD1_REPRESENTATION_URI = "http://localhost:8080/mcs/records/a1/representations/repr/versions/v1";
    private static final String RECORD2_REPRESENTATION_URI = "http://localhost:8080/mcs/records/b2/representations/repr/versions/v1";
    private static final String AUTHORIZATION_HEADER = "Basic abc123";
    private static final Date REVISION_TIMESTAMP = new Date(0);
    private static final String REVISION_PROVIDER = "revisionProvider";
    private static final String REVISION_NAME = "revisionName";
    private static final Revision RESULT_REVISION = new Revision(REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP,
            false, false, true);
    private static final String HARVEST_DATE_STRING = "2021-05-26T08:00:00.000Z";
    private static final Date HARVEST_DATE = DateHelper.parseISODate(HARVEST_DATE_STRING);
    private static final Date OLDER_DATE = DateHelper.parseISODate("2021-05-26T07:30:00.000Z");

    private final DpsTask task = new DpsTask();
    private List<HarvestedRecord> allHarvestedRecords;

    @Mock
    private HarvestedRecordsDAO harvestedRecordsDAO;

    @Mock
    private ProcessedRecordsDAO processedRecordsDAO;

    @Mock
    private RecordServiceClient recordServiceClient;

    @Mock
    private RevisionServiceClient revisionServiceClient;

    @Mock
    private DataSetServiceClient dataSetServiceClient;

    @Mock
    private UISClient uisClient;

    @Mock
    private TaskStatusUpdater taskStatusUpdater;

    @InjectMocks
    private HarvestingPostProcessor service;

    @Before
    public void before() throws CloudException, MCSException, URISyntaxException {
        mockDAOs();
        mockUis();
        mockRecordServiceClient();
        prepareTask();
    }

    private void mockDAOs() {
        allHarvestedRecords = new ArrayList<>();
        when(harvestedRecordsDAO.findDatasetRecords(METIS_DATASET_ID)).thenAnswer(invocation -> allHarvestedRecords.iterator());
        when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.empty());
    }

    private void mockRecordServiceClient() throws MCSException, URISyntaxException {
        when(recordServiceClient.createRepresentation(CLOUD_ID1, REPRESENTATION_NAME, PROVIDER_ID,
                AUTHORIZATION, AUTHORIZATION_HEADER)).thenReturn(new URI(RECORD1_REPRESENTATION_URI));
        when(recordServiceClient.createRepresentation(CLOUD_ID2, REPRESENTATION_NAME, PROVIDER_ID,
                AUTHORIZATION, AUTHORIZATION_HEADER)).thenReturn(new URI(RECORD2_REPRESENTATION_URI));

    }

    private void mockUis() throws CloudException {
        CloudId cloudIdObject1 = createCloudId(CLOUD_ID1, RECORD_ID1);
        CloudId cloudIdObject2 = createCloudId(CLOUD_ID2, RECORD_ID2);
        when(uisClient.getCloudId(PROVIDER_ID, RECORD_ID1, AUTHORIZATION, AUTHORIZATION_HEADER)).thenReturn(cloudIdObject1);
        when(uisClient.getCloudId(PROVIDER_ID, RECORD_ID2, AUTHORIZATION, AUTHORIZATION_HEADER)).thenReturn(cloudIdObject2);
    }

    private void prepareTask() {
        task.setTaskId(TASK_ID);
        task.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
        task.addParameter(PluginParameterKeys.HARVEST_DATE, HARVEST_DATE_STRING);
        task.addParameter(PluginParameterKeys.PROVIDER_ID, PROVIDER_ID);
        task.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, REPRESENTATION_NAME);
        task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION_HEADER);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, OUTPUT_DATA_SETS);
        Revision revision = new Revision();
        revision.setRevisionName(REVISION_NAME);
        revision.setRevisionProviderId(REVISION_PROVIDER);
        revision.setCreationTimeStamp(REVISION_TIMESTAMP);
        task.setOutputRevision(revision);
    }


    @Test
    public void shouldNotFailWhenThereIsNoHarvestedRecords() {

        service.execute(task);

        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    }

    @Test
    public void shouldNotDoAnythingWhenAllRecordsBelongsToCurrentHarvest() {
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(HARVEST_DATE).recordLocalId(RECORD_ID1).build());
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(HARVEST_DATE).recordLocalId(RECORD_ID2).build());

        service.execute(task);

        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
        verifyNoInteractions(uisClient, recordServiceClient, revisionServiceClient, dataSetServiceClient);
    }


    @Test
    public void shouldAddOlderRecordAsDeleted() throws MCSException {
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(OLDER_DATE).recordLocalId(RECORD_ID1).build());

        service.execute(task);

        verify(recordServiceClient).createRepresentation(CLOUD_ID1, REPRESENTATION_NAME, PROVIDER_ID, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(revisionServiceClient).addRevision(CLOUD_ID1, REPRESENTATION_NAME, VERSION, RESULT_REVISION, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(dataSetServiceClient).assignRepresentationToDataSet(PROVIDER_ID, DATASET_ID, CLOUD_ID1, REPRESENTATION_NAME, VERSION, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(processedRecordsDAO).insert(any(ProcessedRecord.class));
        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    }

    @Test
    public void shouldOmitRecordThatIsAlreadyAddedAsDeleted() {
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(OLDER_DATE).recordLocalId(RECORD_ID1).build());
        when(processedRecordsDAO.selectByPrimaryKey(TASK_ID,RECORD_ID1)).
                thenReturn(Optional.of(ProcessedRecord.builder().state(RecordState.SUCCESS).build()));

        service.execute(task);

        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
        verify(processedRecordsDAO, never()).insert(any());
        verifyNoInteractions(uisClient, recordServiceClient, revisionServiceClient, dataSetServiceClient);
    }

    @Test
    public void shouldAddAllOlderRecordAsDeleted() throws MCSException {
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(OLDER_DATE).recordLocalId(RECORD_ID1).build());
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(OLDER_DATE).recordLocalId(RECORD_ID2).build());

        service.execute(task);

        //record1
        verify(recordServiceClient).createRepresentation(CLOUD_ID1, REPRESENTATION_NAME, PROVIDER_ID, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(revisionServiceClient).addRevision(CLOUD_ID1, REPRESENTATION_NAME, VERSION, RESULT_REVISION, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(dataSetServiceClient).assignRepresentationToDataSet(PROVIDER_ID, DATASET_ID, CLOUD_ID1, REPRESENTATION_NAME, VERSION, AUTHORIZATION, AUTHORIZATION_HEADER);
        //record2
        verify(recordServiceClient).createRepresentation(CLOUD_ID2, REPRESENTATION_NAME, PROVIDER_ID, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(revisionServiceClient).addRevision(CLOUD_ID2, REPRESENTATION_NAME, VERSION, RESULT_REVISION, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(dataSetServiceClient).assignRepresentationToDataSet(PROVIDER_ID, DATASET_ID, CLOUD_ID2, REPRESENTATION_NAME, VERSION, AUTHORIZATION, AUTHORIZATION_HEADER);
        //task
        verify(processedRecordsDAO, times(2)).insert(any());
        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    }

    @Test
    public void shouldNotAddRecordThatNotBelongsToCurrentHarvest() throws MCSException {
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(HARVEST_DATE).recordLocalId(RECORD_ID1).build());
        allHarvestedRecords.add(HarvestedRecord.builder().latestHarvestDate(OLDER_DATE).recordLocalId(RECORD_ID2).build());

        service.execute(task);

        verify(recordServiceClient).createRepresentation(CLOUD_ID2, REPRESENTATION_NAME, PROVIDER_ID, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(revisionServiceClient).addRevision(CLOUD_ID2, REPRESENTATION_NAME, VERSION, RESULT_REVISION, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(dataSetServiceClient).assignRepresentationToDataSet(PROVIDER_ID, DATASET_ID, CLOUD_ID2, REPRESENTATION_NAME, VERSION, AUTHORIZATION, AUTHORIZATION_HEADER);
        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
        verify(processedRecordsDAO).insert(any());
        verifyNoMoreInteractions(recordServiceClient, revisionServiceClient, dataSetServiceClient);
    }

    private CloudId createCloudId(String cloudId, String localId) {
        CloudId cloudIdObject1 = new CloudId();
        cloudIdObject1.setId(cloudId);
        LocalId localIdObject = new LocalId();
        localIdObject.setRecordId(localId);
        localIdObject.setProviderId(PROVIDER_ID);
        cloudIdObject1.setLocalId(localIdObject);
        return cloudIdObject1;
    }
}