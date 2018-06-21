package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;

/**
 * Stores data related with one file (usually edm file).
 * This data will be sent to {@link eu.europeana.cloud.dps.topologies.media.StatsBolt} for statistics purposes
 * (successful or failed processing)
 *
 * Created by pwozniak on 6/20/18
 */
public class FileTupleData implements Serializable {

    public static final String STREAM_ID = "file-stats";
    public static final String FIELD_NAME = STREAM_ID;

    public long taskId;
    public String info_text;
    public String resource_url;
    public long resource_no;
    public String topology_name;
    public String state;

}
