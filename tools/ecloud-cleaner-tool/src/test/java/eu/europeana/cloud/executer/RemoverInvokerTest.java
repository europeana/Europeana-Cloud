package eu.europeana.cloud.executer;

import eu.europeana.cloud.api.Remover;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RemoverInvokerTest {

    @Mock(name = "remover")
    private Remover remover;

    @InjectMocks
    private RemoverInvoker removerInvoker;

    private static final long TASK_ID = 1234;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    }


    @Test
    public void shouldInvokeAllTheRemovalStepsIncludingErrorReports() {
        removerInvoker.executeInvokerForSingleTask(TASK_ID, true);
        verify(remover, times(1)).removeNotifications((eq(TASK_ID)));
        verify(remover, times(1)).removeStatistics((eq(TASK_ID)));
        verify(remover, times(1)).removeErrorReports((eq(TASK_ID)));

    }


    @Test
    public void shouldInvokeAllTheRemovalStepsExcludingErrorReports() {
        removerInvoker.executeInvokerForSingleTask(TASK_ID, false);
        verify(remover, times(1)).removeNotifications((eq(TASK_ID)));
        verify(remover, times(1)).removeStatistics((eq(TASK_ID)));
        verify(remover, times(0)).removeErrorReports((eq(TASK_ID)));
    }


    @Test
    public void shouldExecuteTheRemovalOnListOfTASKS() throws IOException {
        removerInvoker.executeInvokerForListOfTasks("src/test/resources/taskIds.csv", true);
        verify(remover, times(6)).removeNotifications(anyLong());
        verify(remover, times(6)).removeStatistics((anyLong()));
        verify(remover, times(6)).removeErrorReports((anyLong()));
    }


}