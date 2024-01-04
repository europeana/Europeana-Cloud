package eu.europeana.cloud.service.dps.storm.utils;

/**
 * Class is responsible for evaluating maximum parallelization limits used on bolt level in media topology. It is needed only for
 * bolts that connect to external servers. We want to throttle overall sum number of threads that could connect to the external
 * server at the same time, cause such a big traffic could kill some servers. There are two kinds of bolt that connects to these
 * servers in MediaTopology: EDMObjectProcessorBolt and ResourceProcessingBolt. Cause both of them could connect to external
 * server in parallel, we need to divide value passed by user in parameter MAX_PARALLELIZATION between these two bolts. It is done
 * in the ratio: 40% for the EDMObjectProcessorBolt and 60% for the ResourceProcessingBolt. The second bolt gets higher values,
 * cause multiple instances of it could be used for one record if the record has many links. The ratio was also estimated, based
 * on some rough statistics, that on average both bolts spend similar time connecting to external server. These ratios could be
 * tuned in the future. Besides, value 1 is a special case. It could not be simply divided between two bolts, cause both of them
 * need to have at least one thread. So in this case the throttling is achieved by additionally limiting spout max pending to 1.
 * Also, numbers lower than 4 are evaluated in changed way due integer mathematics.
 */
public final class MediaThrottlingFractionEvaluator {

  private MediaThrottlingFractionEvaluator() {
  }

  public static int evalForResourceProcessing(int maxParallelization) {
    if (maxParallelization == 1) {
      return 1;
    } else if (maxParallelization < 4) {
      return maxParallelization - 1;
    } else {
      return evalResourceProcessingFraction(maxParallelization);
    }
  }

  public static int evalForEdmObjectProcessing(int maxParallelization) {
    if (maxParallelization < 4) {
      return 1;
    } else {
      return maxParallelization - evalResourceProcessingFraction(maxParallelization);
    }
  }

  private static int evalResourceProcessingFraction(int maxParallelization) {
    return (int) (0.6 * maxParallelization);
  }

}
