package eu.europeana.cloud.tools.statistics;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class TaskStatisticsAccumulatorTest {
    private static final Logger LOG = LoggerFactory.getLogger(StatisticsAccumulatorTest.class);

    private static final String DUMMY_OP = "dummyOperation";

    @Test
    public void shouldWorkWithTwoSingleTasksTest() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, "op1"));
        taskStatisticsAccumulator.process(getLine(false, 1, "op1"));

        assertEquals(0, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(1f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldWorkWithOneTwoThreeThreeTwoOneTest() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, "op0"));
        taskStatisticsAccumulator.process(getLine(true, 1, "op1"));
        taskStatisticsAccumulator.process(getLine(true, 2, "op2"));
        taskStatisticsAccumulator.process(getLine(false, 3, "op0"));
        taskStatisticsAccumulator.process(getLine(false, 3, "op1"));
        taskStatisticsAccumulator.process(getLine(false, 3, "op2"));

        assertEquals(0, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(3, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(2f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeOpenedOperation() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, "op1"));
        taskStatisticsAccumulator.process(getLine(false, 1, "op0"));
        taskStatisticsAccumulator.process(getLine(false, 2, "op1"));

        assertEquals(0, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(1.5f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeUnclosedOperation() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, "op0"));
        taskStatisticsAccumulator.process(getLine(true, 1, "op1"));
        taskStatisticsAccumulator.process(getLine(false, 2, "op0"));

        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(1.5f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeOpenedBreakUnclosedOperation() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(false, 1, "op0"));
        taskStatisticsAccumulator.process(getLine(true, 2, "op1"));


        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(0f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);


        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeOpenedBreakUnclosedOperationWithDummy() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, DUMMY_OP));
        taskStatisticsAccumulator.process(getLine(false, 0, DUMMY_OP));

        taskStatisticsAccumulator.process(getLine(false, 1, "op0"));
        taskStatisticsAccumulator.process(getLine(true, 2, "op1"));

        taskStatisticsAccumulator.process(getLine(true, 2, DUMMY_OP));
        taskStatisticsAccumulator.process(getLine(false, 2, DUMMY_OP));

        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(0.5f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }


    @Test
    public void shouldRecognizeOpenedUnclosedOperation() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(false, 1, "op0"));
        taskStatisticsAccumulator.process(getLine(true, 1, "op1"));


        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(0f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }


    @Test
    public void shouldRecognizeOpenedUnclosedOperationWithDummy() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, DUMMY_OP));
        taskStatisticsAccumulator.process(getLine(false, 0, DUMMY_OP));

        taskStatisticsAccumulator.process(getLine(false, 1, "op0"));
        taskStatisticsAccumulator.process(getLine(true, 1, "op1"));

        taskStatisticsAccumulator.process(getLine(true, 2, DUMMY_OP));
        taskStatisticsAccumulator.process(getLine(false, 2, DUMMY_OP));

        assertEquals(1, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(1f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }


    @Test
    public void shouldRecognizeOpenedWithUnclosedOperation() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 1, "op1"));
        taskStatisticsAccumulator.process(getLine(false, 2, "op0"));


        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(2f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeALotOfRequests() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 1, "a"));
        taskStatisticsAccumulator.process(getLine(true, 2, "b"));
        taskStatisticsAccumulator.process(getLine(false, 3, "a"));
        taskStatisticsAccumulator.process(getLine(true, 4, "c"));
        taskStatisticsAccumulator.process(getLine(true, 5, "d"));
        taskStatisticsAccumulator.process(getLine(true, 6, "e"));
        taskStatisticsAccumulator.process(getLine(false, 7, "d"));
        taskStatisticsAccumulator.process(getLine(false, 8, "e"));
        taskStatisticsAccumulator.process(getLine(false, 9, "b"));
        taskStatisticsAccumulator.process(getLine(true, 10, "f"));
        taskStatisticsAccumulator.process(getLine(false, 11, "f"));
        taskStatisticsAccumulator.process(getLine(false, 12, "c"));

        assertEquals(0, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(4, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(2f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeValidProportions1() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, "a"));
        taskStatisticsAccumulator.process(getLine(true, 2, "b"));
        taskStatisticsAccumulator.process(getLine(true, 2, "c"));
        taskStatisticsAccumulator.process(getLine(true, 2, "d"));
        taskStatisticsAccumulator.process(getLine(true, 2, "e"));
        taskStatisticsAccumulator.process(getLine(false, 3, "e"));
        taskStatisticsAccumulator.process(getLine(false, 3, "d"));
        taskStatisticsAccumulator.process(getLine(false, 3, "c"));
        taskStatisticsAccumulator.process(getLine(false, 3, "b"));
        taskStatisticsAccumulator.process(getLine(false, 4, "a"));

        assertEquals(0, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(5, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(2f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeValidProportions2() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, "a"));
        taskStatisticsAccumulator.process(getLine(true, 0, "b"));
        taskStatisticsAccumulator.process(getLine(false, 3, "b"));
        taskStatisticsAccumulator.process(getLine(true, 3, "c"));
        taskStatisticsAccumulator.process(getLine(false, 4, "c"));
        taskStatisticsAccumulator.process(getLine(false, 4, "a"));

        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(2, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(2f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }

    @Test
    public void shouldRecognizeValidProportions3() {
        var taskStatisticsAccumulator = new TaskStatisticsAccumulator();

        taskStatisticsAccumulator.process(getLine(true, 0, "a"));
        taskStatisticsAccumulator.process(getLine(true, 0, "b"));
        taskStatisticsAccumulator.process(getLine(true, 0, "d"));
        taskStatisticsAccumulator.process(getLine(false, 1, "d"));
        taskStatisticsAccumulator.process(getLine(true, 2, "e"));
        taskStatisticsAccumulator.process(getLine(false, 3, "e"));
        taskStatisticsAccumulator.process(getLine(false, 3, "b"));
        taskStatisticsAccumulator.process(getLine(true, 3, "c"));
        taskStatisticsAccumulator.process(getLine(false, 4, "c"));
        taskStatisticsAccumulator.process(getLine(false, 4, "a"));

        assertEquals(0, taskStatisticsAccumulator.toStatistics().getMin());
        assertEquals(3, taskStatisticsAccumulator.toStatistics().getMax());
        assertEquals(2.5f, taskStatisticsAccumulator.toStatistics().getAvg(), 0);

        LOG.info(taskStatisticsAccumulator.toStatistics().toString());
    }


    private LogLine getLine(boolean startLine, int secondsOffset, String opId) {
        return LogLine.builder()
                .operationBegin(startLine)
                .dateTime(LocalDateTime.of(2022, 1, 1, 12, 0, secondsOffset))
                .opId(opId)
                .opName("opName1")
                .taskId(1L)
                .build();
    }
}
