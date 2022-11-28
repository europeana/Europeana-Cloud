package eu.europeana.cloud.service.dps.storm.throttling;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.MediaThrottlingFractionEvaluator;
import java.util.Random;

/**
 * Class is responsible for generating grouping attribute used for throttling. This allows implements throttling on the bolt
 * level. Special artificial attribute is generated so every tuple for given task is assigned to his group. Only limited number of
 * attribute value is generated for given task based on MAXIMUM_PARALLELIZATION parameter. So tuples from the given task could
 * only go to limited number of bolts. This enforce throttling that is global on topology level in distributed Storm environment,
 * without external synchronization between bolts.
 */
public class ThrottlingTupleGroupSelector {

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
