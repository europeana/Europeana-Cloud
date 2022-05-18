package eu.europeana.cloud.service.dps.storm.utils;

public class MediaThrottlingFractionEvaluator {


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
