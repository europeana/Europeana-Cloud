package eu.europeana.cloud.tools.statistics;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class StatisticsAccumulatorTest {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticsAccumulatorTest.class);

    private static final long TASK_1_ID = 1L;
    private static final long TASK_2_ID = 2L;

    @Test
    public void shouldWorkWithTwoSingleTasksTest() {
        var statisticsAccumulator = new StatisticsAccumulator();

        statisticsAccumulator.process(getLine(TASK_1_ID,true, 0, "op1"));
        statisticsAccumulator.process(getLine(TASK_1_ID,false, 1, "op1"));
        statisticsAccumulator.process(getLine(TASK_2_ID, true, 2, "op1"));
        statisticsAccumulator.process(getLine(TASK_2_ID, false, 3, "op1"));

        assertEquals(0, statisticsAccumulator.toStatistics().get(TASK_1_ID).getMin());
        assertEquals(1, statisticsAccumulator.toStatistics().get(TASK_1_ID).getMax());
        assertEquals(1f, statisticsAccumulator.toStatistics().get(TASK_1_ID).getAvg(), 0);

        assertEquals(0, statisticsAccumulator.toStatistics().get(TASK_2_ID).getMin());
        assertEquals(1, statisticsAccumulator.toStatistics().get(TASK_2_ID).getMax());
        assertEquals(1f, statisticsAccumulator.toStatistics().get(TASK_2_ID).getAvg(), 0);

        LOG.info(statisticsAccumulator.toStatistics().get(TASK_1_ID).toString());
        LOG.info(statisticsAccumulator.toStatistics().get(TASK_2_ID).toString());
    }

    @Test
    public void shouldWorkWithOneTwoThreeThreeTwoOneTest() {
        var statisticsAccumulator = new StatisticsAccumulator();

        statisticsAccumulator.process(getLine(TASK_1_ID, true, 0, "op0"));
        statisticsAccumulator.process(getLine(TASK_2_ID, true, 0, "op0"));
        statisticsAccumulator.process(getLine(TASK_2_ID, true, 0, "op1"));
        statisticsAccumulator.process(getLine(TASK_2_ID, true, 0, "op2"));
        statisticsAccumulator.process(getLine(TASK_1_ID, true, 1, "op1"));
        statisticsAccumulator.process(getLine(TASK_2_ID, false, 1, "op0"));
        statisticsAccumulator.process(getLine(TASK_1_ID, true, 2, "op2"));
        statisticsAccumulator.process(getLine(TASK_2_ID, false, 2, "op1"));
        statisticsAccumulator.process(getLine(TASK_1_ID, false, 3, "op0"));
        statisticsAccumulator.process(getLine(TASK_1_ID, false, 3, "op1"));
        statisticsAccumulator.process(getLine(TASK_1_ID, false, 3, "op2"));
        statisticsAccumulator.process(getLine(TASK_2_ID, false, 3, "op2"));


        assertEquals(0, statisticsAccumulator.toStatistics().get(TASK_1_ID).getMin());
        assertEquals(3, statisticsAccumulator.toStatistics().get(TASK_1_ID).getMax());
        assertEquals(2f, statisticsAccumulator.toStatistics().get(TASK_1_ID).getAvg(), 0);

        assertEquals(0, statisticsAccumulator.toStatistics().get(TASK_2_ID).getMin());
        assertEquals(3, statisticsAccumulator.toStatistics().get(TASK_2_ID).getMax());
        assertEquals(2f, statisticsAccumulator.toStatistics().get(TASK_2_ID).getAvg(), 0);

        LOG.info(statisticsAccumulator.toStatistics().get(TASK_1_ID).toString());
        LOG.info(statisticsAccumulator.toStatistics().get(TASK_2_ID).toString());
    }


    private LogLine getLine(Long taskId, boolean startLine, int secondsOffset, String opId) {
        return LogLine.builder()
                .operationBegin(startLine)
                .dateTime(LocalDateTime.of(2022, 1, 1, 12, 0, secondsOffset))
                .opId(opId)
                .opName("opName1")
                .taskId(taskId)
                .build();
    }
}
