package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.config.CleanTaskDirServiceTestContext;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {CleanTaskDirService.class, CleanTaskDirServiceTestContext.class})
@TestPropertySource(properties = {"harvestingTasksDir="+CleanTaskDirServiceTest.TEST_BASE_DIR})
public class CleanTaskDirServiceTest {
    private static final String TEST_ANY_DIR = "/any/dir";
    static final String TEST_BASE_DIR = "./test_http_harvest";
    private static final long TASK_ID_BOUND = 10000000L;
    private static final int TEST_COUNTER = 1000;
    private static final int SUB_DIRS_COUNT = 3;

    @Autowired
    private CleanTaskDirService cleanTaskDirService;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Before
    public void setup() {
        Mockito.reset(taskInfoDAO);
    }

    @Test
    public void constructValidDir() {
        String dirName1 = CleanTaskDirService.getDirName(TEST_ANY_DIR, TASK_ID_BOUND);
        String dirName2 = CleanTaskDirService.getDirName(TEST_ANY_DIR+ File.separatorChar, TASK_ID_BOUND);

        String dirName3 = CleanTaskDirService.getDirName(TEST_ANY_DIR, -TASK_ID_BOUND);
        String dirName4 = CleanTaskDirService.getDirName(TEST_ANY_DIR+ File.separatorChar, -TASK_ID_BOUND);

        assertEquals(dirName1, dirName2);
        assertEquals(dirName3, dirName4);
    }

    @Test
    public void extractTaskIdFromPath() {
        for(int index = 0; index < TEST_COUNTER; index++) {
            long taskId = ThreadLocalRandom.current().nextLong(-TASK_ID_BOUND, TASK_ID_BOUND);

            String dirName = CleanTaskDirService.getDirName(TEST_ANY_DIR, taskId);
            long restoredTaskId = CleanTaskDirService.getTaskId(new File(dirName));

            assertEquals(taskId, restoredTaskId);
        }
    }

    @Test
    public void removeUnnecessaryTasksDirs() throws IOException {
        int processedDroppedTasksCounter = 0;

        File baseDir = new File(TEST_BASE_DIR);
        FileUtils.forceMkdir(baseDir);
        baseDir.deleteOnExit();

        when(taskInfoDAO.findById(anyLong())).thenReturn(Optional.of(createTaskInfo(anyLong(),  TaskState.PENDING)));

        for(int index = 0; index <TEST_COUNTER; index++) {
            long taskId = ThreadLocalRandom.current().nextLong(-TASK_ID_BOUND, TASK_ID_BOUND);

            String taskDirName = CleanTaskDirService.getDirName(baseDir.getAbsolutePath(), taskId);
            File taskDir = new File(taskDirName);

            FileUtils.forceMkdir(taskDir);
            createDirContents(taskDir);

            TaskState state;
            if(index % 4 == 0) {
                state = TaskState.PROCESSED;
                processedDroppedTasksCounter++;
            } else if(index % 4 == 1) {
                state = TaskState.DROPPED;
                processedDroppedTasksCounter++;
            } else if(index % 4 == 2) {
                state = null;
                processedDroppedTasksCounter++;
            } else {
                state = TaskState.PENDING;   //other than PROCESSED & DROPPED & null
            }

            TaskInfo taskInfo = createTaskInfo(taskId, state);
            when(taskInfoDAO.findById(taskId)).thenReturn(Optional.of(taskInfo));
        }

        int allFilesInDirCounter = Optional.ofNullable(baseDir.list())
                .map(array -> array.length)
                .orElse(0);

        cleanTaskDirService.serviceTask();
        int afterServiceCounter = Optional.ofNullable(baseDir.list())
                .map(array -> array.length)
                .orElse(0);

        FileUtils.forceDelete(baseDir);

        assertEquals(allFilesInDirCounter, afterServiceCounter+processedDroppedTasksCounter);
    }

    private TaskInfo createTaskInfo(long id, TaskState state) {
        TaskInfo result = new TaskInfo();
        result.setId(id);
        result.setState(state);
        return result;
    }

    private void createDirContents(File parentDir) throws IOException {
        for(int index = 0; index < SUB_DIRS_COUNT; index++) {
            File subDir = new File(parentDir, "subdir"+index);
            FileUtils.forceMkdir(subDir);

            File insideFile = new File(parentDir, "insidefile"+index);
            writeFileContent(insideFile);

            File inside2File = new File(subDir, "inside2file"+index);
            writeFileContent(inside2File);
        }
    }

    private void writeFileContent(File file) throws IOException {
        try(FileWriter fw = new FileWriter(file)) {
            fw.write(file.getAbsolutePath());
        }
    }
}
