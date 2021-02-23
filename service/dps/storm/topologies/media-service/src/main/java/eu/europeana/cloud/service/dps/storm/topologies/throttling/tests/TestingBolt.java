package eu.europeana.cloud.service.dps.storm.topologies.throttling.tests;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER;

public class TestingBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TestingBolt.class);
    private static Map<String,StormTaskTuple> active= Collections.synchronizedMap(new TreeMap<>());

    private static final Object o=new Object();
    private static Timer t;
    private static int activeSum;
    private static int averageSteps;
    private long sleepTime;

    public TestingBolt(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        LOGGER.debug("Starting testing bolt for: {}", stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER));
        try {
            add(stormTaskTuple);
            //sleepTime = 3_000L;
            Thread.sleep(sleepTime);
            remove(stormTaskTuple);
        } catch (InterruptedException e) {
            new RuntimeException(e);
        }

        emitResult(anchorTuple,stormTaskTuple);
        outputCollector.ack(anchorTuple);
    }

    protected void emitResult(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    //    Do not emit because it is the only, or the last bolt in topology
    }

    private void add(StormTaskTuple stormTaskTuple) {
        active.put(Thread.currentThread().getName(),stormTaskTuple);
    //    printActiveBoltInstanceOnCurrentWorker();
    }


    private void remove(StormTaskTuple stormTaskTuple) {
        active.remove(Thread.currentThread().getName());
      //  printActiveBoltInstanceOnCurrentWorker();
    }

    private static void printActiveBoltInstanceOnCurrentWorker() {
        synchronized (active) {
            activeSum += active.size();
            averageSteps++;
            double average = activeSum / averageSteps;


            if(LOGGER.isDebugEnabled()) {
                LOGGER.info("Active count: {} , average: {}\n{}", active.size(), average,
                        active.entrySet().stream()
                                .map(e -> "" + e.getKey() + "->" + e.getValue().getTaskId() + "." + e.getValue().getFileUrl())
                                .collect(Collectors.joining("\n")));
            }else{
                LOGGER.info("Active count: {} , average: {}", active.size(), average);
            }
        }

    }


    @Override
    public void prepare() {
        synchronized (o){
            if(t==null){
                 t = new Timer("Printing Bolt A");
                t.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        printActiveBoltInstanceOnCurrentWorker();
                    }
                }, 7000L, 2371L);

            }
        }
    }


}
