package eu.europeana.cloud.service.dps.services.submitters;


import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TaskSubmitterFactoryTest {

    @Test
    void shouldProvideSubmitterForDepublicationTopology() {

        TaskSubmitter taskSubmitter = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder()
                        .taskInfo(TaskInfo.builder()
                                .topologyName(TopologiesNames.DEPUBLICATION_TOPOLOGY)
                                              .build())
                            .build());
    assertTrue(taskSubmitter instanceof DepublicationTaskSubmitter);
  }

    @Test
    void shouldProvideSubmitterForOaiTopology() {

        TaskSubmitter taskSubmitter = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder()
                        .taskInfo(TaskInfo.builder()
                                .topologyName(TopologiesNames.OAI_TOPOLOGY)
                                              .build())
                            .build());
    assertTrue(taskSubmitter instanceof OaiTopologyTaskSubmitter);
  }

    @Test
    void shouldProvideSubmitterForHttpTopology() {

        TaskSubmitter taskSubmitter = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder()
                        .taskInfo(TaskInfo.builder()
                                .topologyName(TopologiesNames.HTTP_TOPOLOGY)
                                              .build())
                            .build());
    assertTrue(taskSubmitter instanceof HttpTopologyTaskSubmitter);
  }

    @Test
    void shouldProvideSubmitterForOtherTopologies() {

        TaskSubmitterFactory taskSubmitterFactory = new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        );

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
            .builder()
            .taskInfo(TaskInfo.builder()
                              .topologyName(TopologiesNames.VALIDATION_TOPOLOGY)
                              .build())
            .build()
    ) instanceof OtherTopologiesTaskSubmitter);

    assertTrue(taskSubmitterFactory.provideTaskSubmitter(
        SubmitTaskParameters
            .builder()
            .taskInfo(TaskInfo.builder()
                              .topologyName(TopologiesNames.INDEXING_TOPOLOGY)
                              .build())
            .build()
    ) instanceof OtherTopologiesTaskSubmitter);

    assertTrue(taskSubmitterFactory.provideTaskSubmitter(
        SubmitTaskParameters
            .builder()
            .taskInfo(TaskInfo.builder()
                              .topologyName(TopologiesNames.ENRICHMENT_TOPOLOGY)
                              .build())
            .build()
    ) instanceof OtherTopologiesTaskSubmitter);

    assertTrue(taskSubmitterFactory.provideTaskSubmitter(
        SubmitTaskParameters
            .builder()
            .taskInfo(TaskInfo.builder()
                              .topologyName(TopologiesNames.NORMALIZATION_TOPOLOGY)
                              .build())
            .build()
    ) instanceof OtherTopologiesTaskSubmitter);

    assertTrue(taskSubmitterFactory.provideTaskSubmitter(
        SubmitTaskParameters
            .builder()
            .taskInfo(TaskInfo.builder()
                              .topologyName(TopologiesNames.LINKCHECK_TOPOLOGY)
                              .build())
            .build()
    ) instanceof OtherTopologiesTaskSubmitter);
    assertTrue(taskSubmitterFactory.provideTaskSubmitter(
        SubmitTaskParameters
            .builder()
            .taskInfo(TaskInfo.builder()
                              .topologyName(TopologiesNames.MEDIA_TOPOLOGY)
                              .build())
            .build()
    ) instanceof OtherTopologiesTaskSubmitter);

        assertTrue(taskSubmitterFactory.provideTaskSubmitter(
                SubmitTaskParameters
                        .builder()
                        .taskInfo(TaskInfo.builder()
                                .topologyName(TopologiesNames.XSLT_TOPOLOGY)
                                .build())
                        .build()
        ) instanceof OtherTopologiesTaskSubmitter);
    }

    @Test
    void shouldThrowExceptionForUnknownTopologyName() {
        assertThrows(IllegalArgumentException.class, () -> new TaskSubmitterFactory(
                Mockito.mock(OaiTopologyTaskSubmitter.class),
                Mockito.mock(HttpTopologyTaskSubmitter.class),
                Mockito.mock(OtherTopologiesTaskSubmitter.class),
                Mockito.mock(DepublicationTaskSubmitter.class)
        ).provideTaskSubmitter(
                SubmitTaskParameters.builder()
                        .taskInfo(TaskInfo.builder()
                                .topologyName("Unknown topology name")
                                .build())
                        .build()));
  }
}