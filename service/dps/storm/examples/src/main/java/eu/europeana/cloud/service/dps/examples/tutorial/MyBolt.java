package eu.europeana.cloud.service.dps.examples.tutorial;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.util.Map;

/**
 * Boilerplate bolt class code to get you started with DPS plugin development
 *
 * @author lucasanastasiou
 */
public class MyBolt extends AbstractDpsBolt {

    @Override
    public void execute(StormTaskTuple t) {
        //
        // your code goes here
        //
        Map<String, String> parameters = t.getParameters();

        outputCollector.emit("stream-to-next-bolt", t.toStormTuple());
    }

    @Override
    public void prepare() {
        //
        // your code goes here
        //
    }
}
