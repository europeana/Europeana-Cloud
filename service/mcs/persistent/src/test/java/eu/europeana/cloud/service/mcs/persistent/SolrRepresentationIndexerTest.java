package eu.europeana.cloud.service.mcs.persistent;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.persistent.util.CompoundDataSetId;
import java.util.HashMap;
import java.util.LinkedHashMap;
import static org.hamcrest.Matchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
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
@ContextConfiguration(value = { "classpath:/rabbitContext.xml" })
public class SolrRepresentationIndexerTest {

    @Autowired
    SolrRepresentationIndexer indexer;

    @Autowired
    RabbitTemplate template;

    private static final Gson gson = new Gson();


    @After
    public void cleanUp() {
        Mockito.reset(template);
    }


    //TODO - implement
    @Ignore
    @Test
    public void shouldSendMessageAboutNotPersistentRepresentationVersionInsert() {

    }


    @Ignore
    @Test
    public void shouldSendMessageAboutPersistentRepresentationVersionInsert() {

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
        String cloudId = "fff123123";
        String schemaId = "rdf";
        HashMap<String, String> map = new LinkedHashMap<>();
        map.put(ParamConstants.F_CLOUDID, cloudId);
        map.put(ParamConstants.F_REPRESENTATIONNAME, schemaId);
        String json = gson.toJson(map);

        //when
        indexer.removeRepresentation(cloudId, schemaId);
        //then
        verify(template, times(1)).convertAndSend("records.representations.versions.deleteVersion", json);
        verifyNoMoreInteractions(template);
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
        verify(template, times(1)).convertAndSend(eq("records.representations.assignments.add"), argument.capture());
        verifyNoMoreInteractions(template);

        JsonObject jo = gson.fromJson(argument.getValue(), JsonElement.class).getAsJsonObject();
        assertThat(jo.get(ParamConstants.P_VER).getAsString(), is(versionId));
        assertTrue(jo.get("compoundDataSetId").isJsonObject());
        assertThat(jo.get("compoundDataSetId").getAsJsonObject().get("dataSetId").getAsString(), is(ds.getDataSetId()));
        assertThat(jo.get("compoundDataSetId").getAsJsonObject().get("dataSetProviderId").getAsString(),
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
        verify(template, times(1)).convertAndSend(eq("records.representations.assignments.delete"), argument.capture());
        verifyNoMoreInteractions(template);

        JsonObject jo = gson.fromJson(argument.getValue(), JsonElement.class).getAsJsonObject();
        assertThat(jo.get(ParamConstants.P_CLOUDID).getAsString(), is(cloudId));
        assertThat(jo.get(ParamConstants.P_REPRESENTATIONNAME).getAsString(), is(representationName));
        assertTrue(jo.get("compoundDataSetId").isJsonObject());
        assertThat(jo.get("compoundDataSetId").getAsJsonObject().get("dataSetId").getAsString(), is(ds.getDataSetId()));
        assertThat(jo.get("compoundDataSetId").getAsJsonObject().get("dataSetProviderId").getAsString(),
            is(ds.getDataSetProviderId()));
    }
}
