package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationBolt extends AbstractDpsBolt {
    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
    }

    @Override
    public void prepare() {

    }


}
