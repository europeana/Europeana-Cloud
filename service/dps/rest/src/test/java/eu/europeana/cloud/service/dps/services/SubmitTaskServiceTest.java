package eu.europeana.cloud.service.dps.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.metis.harvesting.HarvesterException;
import io.gdcc.xoai.serviceprovider.exceptions.HttpException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import org.apache.http.HttpHost;
import org.apache.http.conn.ConnectTimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubmitTaskServiceTest {



  @Mock
  private TaskSubmitterFactory taskSubmitterFactory;
  @Mock
  private TaskStatusUpdater taskStatusUpdater;
  @Mock
  private TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

  @InjectMocks
  private SubmitTaskService service;

  //params
  @Mock
  private SubmitTaskParameters submitTaskParameters;

  @Mock
  private TaskInfo taskInfo;
  @Mock
  private DpsTask dpsTask;
  @Mock
  private TaskSubmitter taskSubmitter;

  @Before
  public void setUp() throws Exception {
   when(submitTaskParameters.getTask()).thenReturn(dpsTask);
    when(taskSubmitterFactory.provideTaskSubmitter(any())).thenReturn(taskSubmitter);

  }

  @Test
  public void testRuntimeException() throws TaskSubmissionException, InterruptedException {
    doThrow(new RuntimeException(catchHarvesertException())).when(taskSubmitter).submitTask(any());
    service.submitTask(submitTaskParameters);
  }

  @Test
  public void testTaskSubmissionException() throws TaskSubmissionException, InterruptedException {
    doThrow(new TaskSubmissionException("Failded",catchHarvesertException())).when(taskSubmitter).submitTask(any());
    service.submitTask(submitTaskParameters);
  }

  private HarvesterException catchHarvesertException() {
    try{
      throwHarveserException();
      return null;
    } catch (HarvesterException e) {
      return e;
    }
  }


  private void throwHarveserException()throws HarvesterException {
    try {
      throwHttpException();
    } catch (HttpException e) {
      throw new HarvesterException(e.getMessage(),e);

    }

  }

  private void throwHttpException() throws HttpException {
    try {
      throwConnectTimeoutException();
    } catch (ConnectTimeoutException e) {
      throw new HttpException(e);
    }
  }

  private void throwConnectTimeoutException() throws ConnectTimeoutException {
    try {
      throwSocketTimeoutException();
    } catch (SocketTimeoutException e) {

      InetAddress address= null;
      try {
        address = InetAddress.getByName("catalonica.bnc.cat");
      } catch (UnknownHostException ex) {
        throw new RuntimeException(ex);
      }
      HttpHost host = new HttpHost(address);
      throw new ConnectTimeoutException(e,host, address);
    }

  }

  private void throwSocketTimeoutException() throws SocketTimeoutException {
    throw new SocketTimeoutException("Connect timed out");
  }
}