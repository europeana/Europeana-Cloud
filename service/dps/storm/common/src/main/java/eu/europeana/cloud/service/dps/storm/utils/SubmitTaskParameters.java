package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static eu.europeana.cloud.service.commons.utils.DateHelper.parseISODate;

/**
 * Set of parameters to submit task to
 */
@Builder
@Getter
@Setter
@ToString
public class SubmitTaskParameters {


    //This incomplete builder implementation is needed to valid field initialization.
    //In absence of this, new class created by Lombok autogenerated builder would have uninitialized fields.
    //Rest functionality of builder is generated automatically by Lombok, according to @Builder annotation.
    public static class SubmitTaskParametersBuilder {
        private AtomicInteger sentRecordsCounter = new AtomicInteger();
        private Date currentHarvestDate=new Date();
    }

    private TaskInfo taskInfo;

    /**
     * Submitting task
     */
    private DpsTask task;

    /**
     * Name of processing topic selected for task
     */
    private String topicName;

    /**
     * Flag if task is subimtted <code>false<code/> or restarted <code>true<code/>
     */
    private boolean restarted;

    private Date currentHarvestDate;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private AtomicInteger sentRecordsCounter;

    public int incrementAndGetPerformedRecordCounter() {
        return sentRecordsCounter.incrementAndGet();
    }

    public String getTaskParameter(String parameterKey){
        return task.getParameter(parameterKey);
    }

    public boolean getUseAlternativeEnvironment() {
        return Boolean.parseBoolean(task.getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV));
    }

    public String getSchemaName() {
        return task.getParameter(PluginParameterKeys.SCHEMA_NAME);
    }

    public String getRepresentationName() {
        return task.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
    }

    public Integer getMaxRecordsCount() {
        return Optional.ofNullable(task.getParameter(PluginParameterKeys.SAMPLE_SIZE)).map(Integer::parseInt).orElse(Integer.MAX_VALUE);
    }

    public RevisionIdentifier getInputRevision() {
        return new RevisionIdentifier(
                task.getParameter(PluginParameterKeys.REVISION_NAME),
                task.getParameter(PluginParameterKeys.REVISION_PROVIDER),
                parseISODate(task.getParameter(PluginParameterKeys.REVISION_TIMESTAMP)));
    }

    public boolean hasInputRevision() {
        return getInputRevision().getRevisionName() != null
                && getInputRevision().getRevisionProviderId() != null;
    }
}
