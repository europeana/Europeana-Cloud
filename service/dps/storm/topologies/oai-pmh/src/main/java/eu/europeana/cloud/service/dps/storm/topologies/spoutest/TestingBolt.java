package eu.europeana.cloud.service.dps.storm.topologies.spoutest;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER;

public class TestingBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TestingBolt.class);
    private static Map<String,StormTaskTuple> active= Collections.synchronizedMap(new TreeMap<>());


    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        LOGGER.info("Starting testing bolt for: {}", stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER));
        try {
            add(stormTaskTuple);
            Thread.sleep(2000L);
            remove(stormTaskTuple);
        } catch (InterruptedException e) {
            new RuntimeException(e);
        }
        outputCollector.ack(anchorTuple);
    }

    private void add(StormTaskTuple stormTaskTuple) {
        active.put(Thread.currentThread().getName(),stormTaskTuple);
        print();
    }


    private void remove(StormTaskTuple stormTaskTuple) {
        active.remove(Thread.currentThread().getName());
        print();
    }

    private void print() {
        synchronized (active) {
            LOGGER.info("Active:\n"+
            active.entrySet().stream()
                    .map(e->""+e.getKey()+"->"+e.getValue().getTaskId()+"."+e.getValue().getFileUrl())
                    .collect(Collectors.joining("\n")));
        }

    }


    @Override
    public void prepare() {

    }


}
