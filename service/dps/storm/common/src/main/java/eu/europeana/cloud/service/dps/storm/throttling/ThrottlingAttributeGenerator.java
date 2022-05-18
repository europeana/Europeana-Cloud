package eu.europeana.cloud.service.dps.storm.throttling;

import java.util.Random;

public class ThrottlingAttributeGenerator {

    Random random=new Random();

    public String generate(long taskId, int maxParallelization) {
        int no = random.nextInt(maxParallelization);
        return taskId + "_" + no;
    }

}
