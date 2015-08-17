package eu.europeana.cloud.service.dps.storm;

import com.google.gson.Gson;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class EndBolt extends AbstractDpsBolt
{

    @Override
    public void execute(StormTaskTuple t) 
    {
        emitSuccessNotification(t.getTaskId(), t.getFileUrl(), "", new Gson().toJson(t));
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() 
    {}  
}
