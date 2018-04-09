package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 4/9/2018.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CassandraTaskInfoDAO.class)
@PowerMockIgnore({"javax.management.*"})
public class MemoryCacheTaskKillerUtilTest {

    private MemoryCacheTaskKillerUtil memoryCacheTaskKillerUtil;
    private CassandraTaskInfoDAO taskInfoDAO;
    private final static long TASK_ID = 1234;

    @Before
    public void init() {
        CassandraConnectionProvider cassandraConnectionProvider = mock(CassandraConnectionProvider.class);

        taskInfoDAO = Mockito.mock(CassandraTaskInfoDAO.class);
        PowerMockito.mockStatic(CassandraTaskInfoDAO.class);
        when(CassandraTaskInfoDAO.getInstance(isA(CassandraConnectionProvider.class))).thenReturn(taskInfoDAO);

        memoryCacheTaskKillerUtil = MemoryCacheTaskKillerUtil.getMemoryCacheTaskKillerUtil(cassandraConnectionProvider);


    }

    @Test
    public void testExecution() throws Exception {
        when(taskInfoDAO.hasKillFlag(TASK_ID)).thenReturn(false, false, false, true, true);
        boolean killedFlag = false;

        for (int i = 0; i < 8; i++) {
            if (i < 4)
                assertFalse(killedFlag);
            killedFlag = memoryCacheTaskKillerUtil.hasKillFlag(TASK_ID);
            Thread.sleep(6000);
        }
        assertTrue(killedFlag);
        verify(taskInfoDAO, times(4)).hasKillFlag(eq(TASK_ID));

    }

}