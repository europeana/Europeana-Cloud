package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas.SchemasSplitter;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas.SchemasSplitterFactory;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.INTERVAL;

/**
 * Created by Tarek on 7/6/2017.
 */
public class Splitter {
    private StormTaskTuple stormTaskTuple;
    private Tuple inputTuple;
    private OutputCollector outputCollector;
    private OAIHelper oaiHelper;
    private long defaultInterval;

    public Splitter(StormTaskTuple stormTaskTuple, Tuple inputTuple, OutputCollector outputCollector, OAIHelper oaiHelper, long defaultInterval) {
        this.stormTaskTuple = stormTaskTuple;
        this.inputTuple = inputTuple;
        this.outputCollector = outputCollector;
        this.oaiHelper = oaiHelper;
        this.defaultInterval = defaultInterval;


    }

    public void splitBySchema() {
        SchemasSplitter schemasSplitter = SchemasSplitterFactory.getTaskSplitter(this);
        schemasSplitter.split();
    }


    public void separateSchemaBySet(String schema, Set<String> sets, Date start, Date end) {
        if (start == null)
            start = oaiHelper.getEarlierDate();
        if (end == null)
            end = new Date();
        if (sets == null || sets.isEmpty()) {
            emitNextTupleByDateRange(schema, null, start, end);
        } else
            for (String set : sets)
                emitNextTupleByDateRange(schema, set, start, end);

    }

    private void emitNextTupleByDateRange(String schema, String set, Date start, Date end) {
        Calendar startCal = Calendar.getInstance();
        startCal.setTime(start);
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new Cloner().deepClone(stormTaskTuple.getSourceDetails());
        setOAIPMHSourceDetails(oaipmhHarvestingDetails, schema, set);
        long interval = getInterval();
        while (start.compareTo(end) <= 0) {
            if ((end.getTime() - start.getTime()) <= interval) {
                OAIPMHHarvestingDetails oaiPmhHarvestingDetailsCloned = buildOAIPMHSourceWithDetailsDateRange(oaipmhHarvestingDetails, start, end);
                outputCollector.emit(inputTuple, buildStormTaskTuple(stormTaskTuple, oaiPmhHarvestingDetailsCloned).toStormTuple());
                break;
            }
            startCal.setTimeInMillis(start.getTime() + interval);
            Date until = startCal.getTime();
            OAIPMHHarvestingDetails oaipmhHarvestingDetails1 = buildOAIPMHSourceWithDetailsDateRange(oaipmhHarvestingDetails, start, until);
            startCal.setTimeInMillis(until.getTime() + 1); //next start = current end +! millisecond
            start = startCal.getTime();
            outputCollector.emit(inputTuple, buildStormTaskTuple(stormTaskTuple, oaipmhHarvestingDetails1).toStormTuple());
        }
    }

    private long getInterval() {
        String providedInterval = stormTaskTuple.getParameter(INTERVAL);
        if (providedInterval != null)
            return Long.parseLong(providedInterval);
        return defaultInterval;
    }

    private void setOAIPMHSourceDetails(OAIPMHHarvestingDetails oaipmhHarvestingDetails, String schema, String set) {
        Set<String> schemas = new HashSet<>();
        schemas.add(schema);
        oaipmhHarvestingDetails.setSchemas(schemas);
        if (set != null) {
            Set<String> sets = new HashSet<>();
            sets.add(set);
            oaipmhHarvestingDetails.setSets(sets);
        }
    }

    private OAIPMHHarvestingDetails buildOAIPMHSourceWithDetailsDateRange(OAIPMHHarvestingDetails oaipmhHarvestingDetails, Date from, Date until) {
        OAIPMHHarvestingDetails oaiPmhHarvestingDetailsCloned = new Cloner().deepClone(oaipmhHarvestingDetails);
        oaiPmhHarvestingDetailsCloned.setDateFrom(from);
        oaiPmhHarvestingDetailsCloned.setDateUntil(until);
        return oaiPmhHarvestingDetailsCloned;
    }

    private StormTaskTuple buildStormTaskTuple(StormTaskTuple t, OAIPMHHarvestingDetails oaipmhHarvestingDetails) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        return stormTaskTuple;
    }

    public OAIHelper getOaiHelper() {
        return oaiHelper;
    }

    public StormTaskTuple getStormTaskTuple() {
        return stormTaskTuple;
    }
}
