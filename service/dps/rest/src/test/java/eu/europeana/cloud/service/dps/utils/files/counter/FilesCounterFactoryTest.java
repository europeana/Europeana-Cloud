package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.TaskSource;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FilesCounterFactoryTest {

  @Mock
  private CassandraTaskInfoDAO taskInfoDAO;
  @Mock
  private IndexWrapper indexWrapper;
  @InjectMocks
  private FilesCounterFactory factory = new FilesCounterFactory(taskInfoDAO, indexWrapper);


  @Test
  void shouldCreateUnknownFilesCounterForHttpTopology() {
    DpsTask task = mock(DpsTask.class);

    FilesCounter counter = factory.createFilesCounter(
        task, TopologiesNames.HTTP_TOPOLOGY);

    assertInstanceOf(UnknownFilesNumberCounter.class, counter);
  }


  @Test
  void shouldCreateOaiFilesCounterForOaiTopology() {
    DpsTask task = mock(DpsTask.class);
    OAIPMHHarvestingDetails harvestingDetails = mock(OAIPMHHarvestingDetails.class);
    when(task.getSource()).thenReturn(harvestingDetails);

    FilesCounter counter = factory.createFilesCounter(task, TopologiesNames.OAI_TOPOLOGY);

    assertInstanceOf(OaiPmhFilesCounter.class, counter);
  }

  @Test
  void shouldCreateDepublicationFilesCounterForDepublicationTopology() {
    DpsTask task = mock(DpsTask.class);

    FilesCounter counter = factory.createFilesCounter(task, TopologiesNames.DEPUBLICATION_TOPOLOGY);

    assertInstanceOf(DepublicationFilesCounter.class, counter);
  }

  @Test
  void shouldCreateDatasetFilesCounterForMCSSourceTopologies() {
    DpsTask task = mock(DpsTask.class);
    BatchInfo batchInfo = mock(BatchInfo.class);
    when(task.getSource()).thenReturn(batchInfo);

    FilesCounter counter = factory.createFilesCounter(task, TopologiesNames.NORMALIZATION_TOPOLOGY);

    assertInstanceOf(DatasetFilesCounter.class, counter);
  }


  @Test
  void shouldCreateUnknownFilesNumberCounterForOthers() {
    DpsTask task = mock(DpsTask.class);
    TaskSource source = mock(TaskSource.class);
    when(task.getSource()).thenReturn(source);

    FilesCounter counter = factory.createFilesCounter(task, "unknownTopology");

    assertInstanceOf(UnknownFilesNumberCounter.class, counter);
  }


}