package eu.europeana.cloud.service.dps.storm.throttling;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.MediaThrottlingFractionEvaluator;

import java.util.Random;

public class ThrottlingAttributeGenerator {

    Random random = new Random();

    public String generateForEdmObjectProcessingBolt(StormTaskTuple tuple) {
        return generate(tuple.getTaskId(),
                MediaThrottlingFractionEvaluator.evalForEdmObjectProcessing(tuple.readParallelizationParam()));
    }

    public String generateForResourceProcessingBolt(StormTaskTuple tuple) {
        return generate(tuple.getTaskId(),
                MediaThrottlingFractionEvaluator.evalForResourceProcessing(tuple.readParallelizationParam()));
    }

    private String generate(long taskId, int maxParallelization) {
        int no = random.nextInt(maxParallelization);
        return taskId + "_" + no;
    }

}
