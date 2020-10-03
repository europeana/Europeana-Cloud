package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.config.CleanTaskDirServiceTestContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {CleanTaskDirService.class, CleanTaskDirServiceTestContext.class})
@TestPropertySource(properties = {"harvestingTasksDir=/home/arek"})
public class CleanTaskDirServiceTest {
    private static final String TEST_BASE_DIR = "/some/path";
    private static final long TASK_ID_BOUND = 10000000L;
    private static final int TEST_COUNTER = 100;

    @Test
    public void constructValidDir() {
        String dirName1 = CleanTaskDirService.getDirName(TEST_BASE_DIR, TASK_ID_BOUND);
        String dirName2 = CleanTaskDirService.getDirName(TEST_BASE_DIR+ File.separatorChar, TASK_ID_BOUND);

        String dirName3 = CleanTaskDirService.getDirName(TEST_BASE_DIR, -TASK_ID_BOUND);
        String dirName4 = CleanTaskDirService.getDirName(TEST_BASE_DIR+ File.separatorChar, -TASK_ID_BOUND);

        assertEquals(dirName1, dirName2);
        assertEquals(dirName3, dirName4);
    }

    @Test
    public void extractTaskIdFromPath() {
        for(int index = 0; index < TEST_COUNTER; index++) {
            long taskId = ThreadLocalRandom.current().nextLong(-TASK_ID_BOUND, TASK_ID_BOUND);

            String dirName = CleanTaskDirService.getDirName(TEST_BASE_DIR, taskId);
            long restoredTaskId = CleanTaskDirService.getTaskId(new File(dirName));

            assertEquals(taskId, restoredTaskId);
        }
    }

    @Test
    public void removeUnnecessaryTasksDirs() {
        //TODO
        assertTrue(true);
    }
}
