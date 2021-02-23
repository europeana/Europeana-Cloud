package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 *  Our own grouping implementation - doing throttling, should resolve problem with using field groping for throttling
 *  investigated in MET-4630. It is only proof of concept implementation. It has hard coded throttling parameter value.
 *  It is not at all adjusted to more than one tasks executed on the given topology (they would use the same bolts, but
 *  should finish properly anyway). It does not distribute tasks evenly through the bolts and server instances
 *  but assumes constant topology size. And anyway the solution is not much studied, so could contain other problems.
 */
public class ThrottlingShuffleGrouping implements CustomStreamGrouping, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottlingShuffleGrouping.class);
    private ArrayList<List<Integer>> choices;
    private static AtomicLong current;

    public ThrottlingShuffleGrouping(){
        System.err.println("CCCCCCCCCCCCCCCCC");
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        LOGGER.info("Prepared grouping: {}, task count: {}, context: {}, stream: {}, available tasks ids: {}",
                System.identityHashCode(this),targetTasks.size(), context, stream, targetTasks);
        choices = new ArrayList<List<Integer>>(targetTasks.size());
        for (Integer i : targetTasks) {
            choices.add(Arrays.asList(i));
        }
        current = new AtomicLong(0);
        Collections.shuffle(choices, new Random());
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        Integer throttlingAttribute = 240;
        int size = throttlingAttribute != null && throttlingAttribute <= choices.size() ? throttlingAttribute : choices.size();
        long rightNow = current.incrementAndGet();
        int choicedIndex = (int) (rightNow % size);
        List<Integer> choicedTaskId = choices.get(choicedIndex);
        LOGGER.info("Selected task by grouping: {}, : {}, index: {}, size: {}, throttlingAttribute: {}, choices count: {}",
                System.identityHashCode(this), choicedTaskId, choicedIndex, size, throttlingAttribute, choices.size());
        return choicedTaskId;
    }

}
