package eu.europeana.cloud.service.dps.storm.io;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.URISyntaxException;
import java.util.Optional;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ReadFileBoltTest {

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock(name = "fileClient")
  private FileServiceClient fileServiceClient;

  private final int retryAttemptsCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(8);

  @InjectMocks
  private ReadFileBolt readFileBolt = new ReadFileBolt(new CassandraProperties(), "MCS_URL", "user", "password");

  private StormTaskTuple stormTaskTuple;
  private static final String FILE_URL = "http://127.0.0.1:8080/mcs/records/BSJD6UWHYITSUPWUSYOVQVA4N4SJUKVSDK2X63NLYCVB4L3OXKOA/representations/NEW_REPRESENTATION_NAME/versions/c73694c0-030d-11e6-a5cb-0050568c62b8/files/dad60a17-deaa-4bb5-bfb8-9a1bbf6ba0b2";

  @Before
  public void init() throws IllegalAccessException, MCSException, URISyntaxException {
    MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    stormTaskTuple = prepareTuple();
  }

  @Test
  public void shouldEmmitNotificationWhenDataSetListHasOneElement() throws MCSException {
    //given
    when(fileServiceClient.getFile(FILE_URL)).thenReturn(null);
    verifyMethodExecutionNumber(1, 0, FILE_URL);
  }

  @Test
  public void shouldRetryBeforeFailingWhenThrowingMCSException() throws MCSException {
    //given
    doThrow(MCSException.class).when(fileServiceClient).getFile(FILE_URL);
    verifyMethodExecutionNumber(retryAttemptsCount, 1, FILE_URL);
  }

  @Test
  public void shouldRetryBeforeFailingWhenThrowingDriverException() throws MCSException {
    //given
    doThrow(DriverException.class).when(fileServiceClient).getFile(FILE_URL);
    verifyMethodExecutionNumber(retryAttemptsCount, 1, FILE_URL);
  }

  private void verifyMethodExecutionNumber(int expectedCalls, int expectedEmitCallTimes, String file) throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    when(outputCollector.emit(anyList())).thenReturn(null);
    readFileBolt.execute(anchorTuple, stormTaskTuple);
    verify(fileServiceClient, times(expectedCalls)).getFile(file);
    verify(outputCollector, times(expectedEmitCallTimes)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class),
        anyList());

  }

  private StormTaskTuple prepareTuple() {
    stormTaskTuple = new StormTaskTuple();
    stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, FILE_URL);
    stormTaskTuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return stormTaskTuple;
  }
}