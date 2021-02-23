package eu.europeana.cloud.service.dps.storm.topologies.throttling.tests;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.throttling.ThrottlingTupleGroupSelector;
import org.apache.storm.tuple.Tuple;

public class TestingIntermediateBolt extends TestingBolt {
    private ThrottlingTupleGroupSelector generator;

    public TestingIntermediateBolt(long sleepTime) {
        super(sleepTime);
    }

    protected void emitResult(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        stormTaskTuple.setThrottlingGroupingAttribute(generator.generateForResourceProcessingBolt(stormTaskTuple));
        outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
    }


    @Override
    public void prepare() {
        super.prepare();
        generator=new ThrottlingTupleGroupSelector();
    }

}
