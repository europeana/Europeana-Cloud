package eu.europeana.cloud.client.dps.rest;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.exception.DpsException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * This class was made with intention to have easy way to run rest requests to DPS application.<br/> This is intentionally
 * annotated with @Ignore annotation because it should be ran by hand by the developer.<br/> In the future we will extend it by
 * adding more integration test. Probably we will also create separate module for integration tests.<br/>
 */
@Disabled
class DPSClientTestIT {

  private static final String DPS_LOCATION = "http://127.0.0.1:8080/services";
  private static final String USER = "user";
  private static final String PASSWORD = "password";

  @Test
  void submitOaiTask() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);
    DpsTask task = new DpsTask();
    task.addParameter("PROVIDER_ID", "metis_test5");
    OAIPMHHarvestingDetails details = new OAIPMHHarvestingDetails();
    details.setRepositoryUrl("http://test117.ait.co.at/oai-provider-edm/oai/");
    details.setSchema("edm");
    details.setSet("ZFMK");
    task.setInput(details);
    DpsTask resultTask = client.createTask(task, "oai_topology");
    long id = resultTask.getTaskId();
    assertNotEquals(0, id);

    TaskInfo taskProgress = client.getTaskProgress("oai_topology", id);
    assertThat(taskProgress.getId(), is(id));
  }

  @Test
  void submitValidationTask() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);
    DpsTask task = new DpsTask();
    DatasetRevisionInfo inputData = DatasetRevisionInfo.builder().providerId("metis_test5")
                                                       .datasetId("f1ffd107-bf85-4a4f-948f-2a8e70ba6b82")
                                                       .representationName("metadataRecord")
                                                       .build();

    task.setInput(inputData);
    task.addParameter("SCHEMA_NAME", "EDM-EXTERNAL");
    DatasetRevisionInfo output = new DatasetRevisionInfo();
    output.setRepresentationName("metadataRecord");
    inputData.setRevision(Revision.builder().
                                  revisionName("OAIPMH_HARVEST")
                                  .revisionProviderId("metis_test5")
                                  .creationTimeStamp("2018-01-31T11:33:30.842+01:00")
                                  .build());
    //
    output.setRevision(Revision.builder().
                               revisionName("VALIDATION_EXTERNAL_TEST")
                               .revisionProviderId("metis_test5")
                               .creationTimeStamp(new Date())
                               .build());
    task.setOutput(output);
    //
    DpsTask resultTask = client.createTask(task, "validation_topology");
    long id = resultTask.getTaskId();
    assertNotEquals(0, id);

    TaskInfo taskProgress = client.getTaskProgress("validation_topology", id);
    assertThat(taskProgress.getId(), is(id));
  }

  @Test
  void shouldReadTaskProgress() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);

    TaskInfo taskProgress = client.getTaskProgress("oai_topology", 3289416056779392187L);
    assertThat(taskProgress.getId(), is(3289416056779392187L));
  }

  @Test
  void shouldCheckIfErrorReportExists() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);

    boolean errorReportExists = client.checkIfErrorReportExists("oai_topology", 3289416056779392187L);
    assertThat(errorReportExists, is(false));
  }

  @Test
  void shouldReadDetailedTaskReport() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);
    List<SubTaskInfo> detailedTaskReport = client.getDetailedTaskReport("enrichment_topology", -6984909380771953612L);
    assertThat(detailedTaskReport, is(notNullValue()));
  }

  @Test
  void shouldReadDetailedTaskReportBetweenChunks() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);
    List<SubTaskInfo> detailedTaskReport = client.getDetailedTaskReportBetweenChunks("enrichment_topology", -6984909380771953612L,
            0, 10);
    assertThat(detailedTaskReport, is(notNullValue()));
  }

  @Test
  void shouldReadElementReport() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);
    List<NodeReport> elementReport = client.getElementReport("enrichment_topology", -6984909380771953612L, "element");
    assertThat(elementReport, is(notNullValue()));
  }

  @Test
  void shouldReadTaskErrorReport() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);
    TaskErrorsInfo taskErrorsReport =
            client.getTaskErrorsReport(
                    "validation_topology",
                    1083958946756839468L,
                    "6716d620-176f-11ea-a1c9-fa163efcf5a8", 100);
    assertThat(taskErrorsReport, is(notNullValue()));
  }

  @Test
  void shouldReadStatisticsReport() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);

    StatisticsReport statsReport = client.getTaskStatisticsReport(
            "validation_topology",
            1083958946756839468L);

    assertThat(statsReport, is(notNullValue()));
  }

  @Test
  void shouldKillTask() throws DpsException {
    DpsClient client = new DpsClient(DPS_LOCATION, USER, PASSWORD);

    String killTask = client.killTask("oai_topology", 1494383335175835194L, "Custom killing procedure");
    assertThat(killTask, is(notNullValue()));
    assertThat(killTask, containsString("Custom killing procedure"));
  }

}
