package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;


public class TaskSubmitterFactoryTest {

    @Test
    public void shouldProvideSubmitterForDepublicationTopology() {

        TaskSubmitter taskSubmitter = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder().topologyName(TopologiesNames.DEPUBLICATION_TOPOLOGY).build());
        assertTrue(taskSubmitter instanceof DepublicationTaskSubmitter);
    }

    @Test
    public void shouldProvideSubmitterForOaiTopology() {

        TaskSubmitter taskSubmitter = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder().topologyName(TopologiesNames.OAI_TOPOLOGY).build());
        assertTrue(taskSubmitter instanceof OaiTopologyTaskSubmitter);
    }

    @Test
    public void shouldProvideSubmitterForHttpTopology() {

        TaskSubmitter taskSubmitter = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder().topologyName(TopologiesNames.HTTP_TOPOLOGY).build());
        assertTrue(taskSubmitter instanceof HttpTopologyTaskSubmitter);
    }

    @Test
    public void shouldProvideSubmitterForOtherTopologies() {

        TaskSubmitterFactory taskSubmitterFactory = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        );

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .topologyName(TopologiesNames.VALIDATION_TOPOLOGY)
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .topologyName(TopologiesNames.INDEXING_TOPOLOGY)
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .topologyName(TopologiesNames.ENRICHMENT_TOPOLOGY)
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .topologyName(TopologiesNames.NORMALIZATION_TOPOLOGY)
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .topologyName(TopologiesNames.LINKCHECK_TOPOLOGY)
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);
        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .topologyName(TopologiesNames.MEDIA_TOPOLOGY)
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .topologyName(TopologiesNames.XSLT_TOPOLOGY)
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForUnknownTopologyName() {
        new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder().topologyName("Unknown topology name").build());
    }
}