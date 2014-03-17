package eu.europeana.cloud.service.mcs.persistent;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import java.lang.reflect.Type;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import junit.framework.Assert;
import org.apache.commons.lang.StringUtils;
import static org.hamcrest.Matchers.is;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.eq;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/rabbitContext.xml"})
public class SolrRepresentationIndexerTest {

    @Autowired
    SolrRepresentationIndexer indexer;

    @Autowired
    RabbitTemplate template;

    @Autowired
    CassandraDataSetDAO cassandraDataSetDAO;

    private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").create();

    @After
    public void cleanUp() {
        Mockito.reset(template);
    }

    @Test
    public void shouldSendMessageAboutNotPersistentRepresentationVersionInsert() {

        String providerId = "Provider001";
        String cloudId = "25DG622J4VM";
        String representationName = "representation01";
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        ArrayList<File> files = new ArrayList<>();
        boolean persistent = false;
        Date creationDate = Calendar.getInstance().getTime();

        Representation representation = new Representation(cloudId, representationName, versionId, null, null,
                providerId, files, persistent, creationDate);
        indexer.insertRepresentation(representation);

        verify(template, times(1)).convertAndSend("records.representations.versions.add", gson.toJson(representation));
        verifyNoMoreInteractions(template);

    }

    @Test
    public void shouldSendMessageAboutPersistentRepresentationVersionInsert() {
        //given
        String providerId = "Provider001";
        String cloudId = "25DG622J4VM";
        String representationName = "representation01";
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        ArrayList<File> files = new ArrayList<>();
        boolean persistent = true;
        Date creationDate = Calendar.getInstance().getTime();
        Representation representation = new Representation(cloudId, representationName, versionId, null, null,
                providerId, files, persistent, creationDate);

        String dataSetId1 = "dataSet1";
        String dataSetId2 = "dataSet2";
        Collection<CompoundDataSetId> dataSetIds = new ArrayList<>();
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId1));
        dataSetIds.add(new CompoundDataSetId(providerId, dataSetId2));

        //when
        Mockito.when(cassandraDataSetDAO.getDataSetAssignments(cloudId, representationName, null)).thenReturn(
                dataSetIds);
        indexer.insertRepresentation(representation);

        //then
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        verify(template, times(1)).convertAndSend(eq("records.representations.versions.addPersistent"),
                argument.capture());
        verifyNoMoreInteractions(template);

        JsonElement jsonElem = gson.fromJson(argument.getValue(), JsonElement.class);
        JsonObject jsonObject = jsonElem.getAsJsonObject();

        JsonElement jsonRepresentation = jsonObject.get(ParamConstants.F_REPRESENTATION);
        Representation sentRepresentation = gson.fromJson(jsonRepresentation, Representation.class);

        Type dataSetIdsType = new TypeToken<Collection<CompoundDataSetId>>() {
        }.getType();
        JsonElement jsonDataSetIds = jsonObject.get(ParamConstants.F_DATASETS);
        Collection<CompoundDataSetId> sentDataSetIds = gson.fromJson(jsonDataSetIds, dataSetIdsType);

        assertEquals(sentDataSetIds, dataSetIds);
        assertEquals(sentRepresentation, representation);

    }

    @Test
    public void shouldSendMessageAboutRepresentationVersionRemoval() {
        //given
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";

        //when
        indexer.removeRepresentationVersion(versionId);
        //then
        verify(template, times(1)).convertAndSend("records.representations.versions.deleteVersion", versionId);
        verifyNoMoreInteractions(template);
    }

    @Test
    public void shouldSendMessageAboutRepresentationRemoval() {
        //given
        String cloudId = "cloudId123123";
        String representationName = "rdf";
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        //when
        indexer.removeRepresentation(cloudId, representationName);
        //then
        verify(template, times(1)).convertAndSend(eq("records.representations.delete"), argument.capture());
        verifyNoMoreInteractions(template);

        JsonObject jo = gson.fromJson(argument.getValue(), JsonElement.class).getAsJsonObject();
        JsonElement cloudIdJson = jo.get(ParamConstants.P_CLOUDID);
        String sentCloudId = cloudIdJson.getAsString();
        assertEquals(sentCloudId, cloudId);

        JsonElement representationNameJson = jo.get(ParamConstants.P_REPRESENTATIONNAME);
        String sentRepresentationName = representationNameJson.getAsString();
        assertEquals(sentRepresentationName, representationName);
    }

    @Test
    public void shouldSendMessageAboutNewAssignment() {
        //given
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";
        CompoundDataSetId ds = new CompoundDataSetId("providerId", "someDataSetId");
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

        //when
        indexer.addAssignment(versionId, ds);

        //then
        verify(template, times(1)).convertAndSend(eq("datasets.assignments.add"), argument.capture());
        verifyNoMoreInteractions(template);

        JsonObject jo = gson.fromJson(argument.getValue(), JsonElement.class).getAsJsonObject();
        assertThat(jo.get(ParamConstants.P_VER).getAsString(), is(versionId));
        assertTrue(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).isJsonObject());
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).getAsJsonObject().get("dataSetId").getAsString(), is(ds.getDataSetId()));
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).getAsJsonObject().get("dataSetProviderId").getAsString(),
                is(ds.getDataSetProviderId()));
    }

    @Test
    public void shouldSendMessageAboutAssignmentRemoval() {
        //given
        String cloudId = "b95fcda053f6012";
        String representationName = "schema";
        CompoundDataSetId ds = new CompoundDataSetId("providerId", "someDataSetId");
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);

        //when
        indexer.removeAssignment(cloudId, representationName, ds);

        //then
        verify(template, times(1)).convertAndSend(eq("datasets.assignments.delete"), argument.capture());
        verifyNoMoreInteractions(template);

        JsonObject jo = gson.fromJson(argument.getValue(), JsonElement.class).getAsJsonObject();
        assertThat(jo.get(ParamConstants.P_CLOUDID).getAsString(), is(cloudId));
        assertThat(jo.get(ParamConstants.P_REPRESENTATIONNAME).getAsString(), is(representationName));
        assertTrue(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).isJsonObject());
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).getAsJsonObject().get("dataSetId").getAsString(), is(ds.getDataSetId()));
        assertThat(jo.get(ParamConstants.F_DATASET_PROVIDER_ID).getAsJsonObject().get("dataSetProviderId").getAsString(),
                is(ds.getDataSetProviderId()));
    }

    @Test
    public void shouldSendMessageAboutRecordsRepresentationsDeletion() {
        //given
        String cloudId = "b95fcda053f6012";
        //when
        indexer.removeRecordRepresentations(cloudId);
        //then
        verify(template, times(1)).convertAndSend("records.representations.deleteAll", cloudId);
        verifyNoMoreInteractions(template);
    }

    @Test
    public void shouldSendMessageAboutAllDataSetAssignmentsDeletion() {
        //given
        CompoundDataSetId compoundDataSetId = new CompoundDataSetId("provider", "dataSet");
        //when
        indexer.removeAssignmentsFromDataSet(compoundDataSetId);
        //then
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        verify(template, times(1)).convertAndSend(eq("datasets.assignments.deleteAll"), argument.capture());
        verifyNoMoreInteractions(template);

        CompoundDataSetId sentCompoundDataSetId = gson.fromJson(argument.getValue(), CompoundDataSetId.class);
        assertEquals(sentCompoundDataSetId, compoundDataSetId);

    }
}
