package eu.europeana.cloud.service.mcs.persistent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.kafka.ProducerWrapper;
import eu.europeana.cloud.service.mcs.messages.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import org.apache.commons.lang.SerializationUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/rabbitContext.xml"})
public class SolrRepresentationIndexerTest {

    @Autowired
    SolrRepresentationIndexer indexer;

    @Autowired
    ProducerWrapper producerWrapper;

    @Autowired
    CassandraDataSetDAO cassandraDataSetDAO;

    private static final Gson gson = new GsonBuilder().setDateFormat(
            "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").create();

    @After
    public void cleanUp() {
        Mockito.reset(producerWrapper);
    }

    @Test
    public void shouldSendMessageAboutNotPersistentRepresentationVersionInsert() {

        String providerId = "Provider001";
        String cloudId = "25DG622J4VM";
        String representationName = "representation01";
        String versionId
                = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        int partitionKey = 32151;
        ArrayList<File> files = new ArrayList<>();
        boolean persistent = false;
        Date creationDate = Calendar.getInstance().getTime();

        Representation representation = new Representation(cloudId,
                representationName, versionId, null, null, providerId, files, null, persistent,
                creationDate);
        indexer.insertRepresentation(representation,
                partitionKey);

        verify(producerWrapper, times(1)).send(partitionKey, SerializationUtils
                .serialize(new InsertRepresentationMessage(gson.toJson(representation))));
        verifyNoMoreInteractions(producerWrapper);
    }

    @Test
    public void shouldSendMessageAboutPersistentRepresentationVersionInsert() {
//given
        String providerId = "Provider001";
        String cloudId = "25DG622J4VM";
        String representationName = "representation01";
        String versionId
                = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        int partitionKey = 32151;
        ArrayList<File> files = new ArrayList<>();
        ArrayList<Revision> revisions = new ArrayList<>();
        boolean persistent = true;
        Date creationDate = Calendar.getInstance().getTime();
        Representation representation = new Representation(cloudId, representationName,
                versionId, null, null, providerId, files, revisions, persistent, creationDate);

        String dataSetId1 = "dataSet1";
        String dataSetId2 = "dataSet2";
        Collection<CompoundDataSetId> dataSetIds = new ArrayList<>();
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId1));
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId2));

        //when 
        Mockito.when(cassandraDataSetDAO.getDataSetAssignments(cloudId,
                representationName, null)).thenReturn(dataSetIds);
        indexer.insertRepresentation(representation, partitionKey);

        //then 
        ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);

        verify(producerWrapper, times(1)).send(eq(partitionKey),
                argument.capture());
        verifyNoMoreInteractions(producerWrapper);

        InsertRepresentationPersistentMessage resultMessage
                = (InsertRepresentationPersistentMessage) SerializationUtils.deserialize((byte[]) (argument.getValue()));

        JsonElement jsonElem = gson.fromJson(resultMessage.getPayload(),
                JsonElement.class);
        JsonObject jsonObject = jsonElem.getAsJsonObject();

        JsonElement jsonRepresentation
                = jsonObject.get(ParamConstants.F_REPRESENTATION);
        Representation sentRepresentation = gson.fromJson(jsonRepresentation,
                Representation.class);

        Type dataSetIdsType = new TypeToken<Collection<CompoundDataSetId>>() {
        }.getType();
        JsonElement jsonDataSetIds
                = jsonObject.get(ParamConstants.F_DATASETS);
        Collection<CompoundDataSetId> sentDataSetIds = gson.fromJson(jsonDataSetIds, dataSetIdsType);

        assertEquals(sentDataSetIds, dataSetIds);
        assertEquals(sentRepresentation, representation);
    }

    @Test
    public void shouldSendMessageAboutRepresentationVersionRemoval() {
        // given
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        int partitionKey = 32151; // when
        indexer.removeRepresentationVersion(versionId, partitionKey);
        // then
        verify(producerWrapper, times(1)).send(
                partitionKey,
                SerializationUtils
                        .serialize(new RemoveRepresentationVersionMessage(
                                versionId)));
        verifyNoMoreInteractions(producerWrapper);
    }

    @Test
    public void shouldSendMessageAboutRepresentationRemoval() {
        // given
        String cloudId = "cloudId123123";
        String representationName = "rdf";
        int partitionKey = 32151;
        ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class); // when
        indexer.removeRepresentation(cloudId, representationName, partitionKey);
        // then
        verify(producerWrapper, times(1)).send(eq(partitionKey),
                argument.capture());
        verifyNoMoreInteractions(producerWrapper);

        RemoveRepresentationMessage sentMessage = (RemoveRepresentationMessage) SerializationUtils
                .deserialize((byte[]) (argument.getValue()));

        JsonObject jo = gson.fromJson(sentMessage.getPayload(),
                JsonElement.class).getAsJsonObject();
        JsonElement cloudIdJson = jo.get(ParamConstants.P_CLOUDID);
        String sentCloudId = cloudIdJson.getAsString();
        assertEquals(sentCloudId, cloudId);

        JsonElement representationNameJson = jo
                .get(ParamConstants.P_REPRESENTATIONNAME);
        String sentRepresentationName = representationNameJson.getAsString();
        assertEquals(sentRepresentationName, representationName);
    }

    @Test
    public void shouldSendMessageAboutNewAssignment() {
        // given
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        int partitionKey = 32151;
        CompoundDataSetId ds = new CompoundDataSetId("providerId",
                "someDataSetId");
        ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);

        // when
        indexer.addAssignment(versionId, ds, partitionKey);

        // then
        verify(producerWrapper, times(1)).send(eq(partitionKey),
                argument.capture());
        verifyNoMoreInteractions(producerWrapper);

        AddAssignmentMessage sentMessage = (AddAssignmentMessage) SerializationUtils
                .deserialize((byte[]) (argument.getValue()));

        JsonObject jo = gson.fromJson(sentMessage.getPayload(),
                JsonElement.class).getAsJsonObject();
        assertThat(jo.get(ParamConstants.P_VER).getAsString(), is(versionId));
        assertTrue(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).isJsonObject());
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID)
                        .getAsJsonObject().get("dataSetId").getAsString(),
                is(ds.getDataSetId()));
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID)
                        .getAsJsonObject().get("dataSetProviderId").getAsString(),
                is(ds.getDataSetProviderId()));
    }

    @Test
    public void shouldSendMessageAboutAssignmentRemoval() {
        // given
        String cloudId = "b95fcda053f6012";
        String representationName = "schema";
        int partitionKey = 32151;
        CompoundDataSetId ds = new CompoundDataSetId("providerId",
                "someDataSetId");
        ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);

        // when
        indexer.removeAssignment(cloudId, representationName, ds, partitionKey);

        // then
        verify(producerWrapper, times(1)).send(eq(partitionKey),
                argument.capture());
        verifyNoMoreInteractions(producerWrapper);

        RemoveAssignmentMessage sentMessage = (RemoveAssignmentMessage) SerializationUtils
                .deserialize((byte[]) (argument.getValue()));

        JsonObject jo = gson.fromJson(sentMessage.getPayload(),
                JsonElement.class).getAsJsonObject();
        assertThat(jo.get(ParamConstants.P_CLOUDID).getAsString(), is(cloudId));
        assertThat(jo.get(ParamConstants.P_REPRESENTATIONNAME).getAsString(),
                is(representationName));
        assertTrue(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).isJsonObject());
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID)
                        .getAsJsonObject().get("dataSetId").getAsString(),
                is(ds.getDataSetId()));
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID)
                        .getAsJsonObject().get("dataSetProviderId").getAsString(),
                is(ds.getDataSetProviderId()));
    }

    @Test
    public void shouldSendMessageAboutRecordsRepresentationsDeletion() { // given
        String cloudId = "b95fcda053f6012";
        int partitionKey = 32151;
        // when
        indexer.removeRecordRepresentations(cloudId, partitionKey);
        // then
        verify(producerWrapper, times(1)).send(
                partitionKey,
                SerializationUtils
                        .serialize(new RemoveRecordRepresentationsMessage(
                                cloudId)));
        verifyNoMoreInteractions(producerWrapper);
    }

    @Test
    public void shouldSendMessageAboutAllDataSetAssignmentsDeletion() {
        // given
        int partitionKey = 32151;
        CompoundDataSetId compoundDataSetId = new CompoundDataSetId("provider",
                "dataSet");
        // when
        indexer.removeAssignmentsFromDataSet(compoundDataSetId, partitionKey);
        // then
        ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);

        verify(producerWrapper, times(1)).send(eq(partitionKey),
                argument.capture());
        verifyNoMoreInteractions(producerWrapper);

        RemoveAssignmentsFromDataSetMessage sentMessage = (RemoveAssignmentsFromDataSetMessage) SerializationUtils
                .deserialize((byte[]) (argument.getValue()));

        CompoundDataSetId sentCompoundDataSetId = gson.fromJson(
                sentMessage.getPayload(), CompoundDataSetId.class);
        assertEquals(sentCompoundDataSetId, compoundDataSetId);
    }
}
