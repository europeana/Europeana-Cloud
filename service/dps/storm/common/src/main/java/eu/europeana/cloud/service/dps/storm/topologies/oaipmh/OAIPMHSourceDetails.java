package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import java.util.Date;
import java.util.List;
import java.util.Set;

public class OAIPMHSourceDetails {

    /** OAI-PMH endpoint URL */
    private String url;

    /** Schema to harvest */
    private String schema;

    /** Sets to harvest */
    private Set<String> sets = null;

    /** Sets to exclude */
    private Set<String> excludeSets = null;

    /** From date */
    private Date dateFrom = null;

    /** Until date */
    private Date dateUntil = null;

    public OAIPMHSourceDetails(String url, String schema) {
        this.url = url;
        this.schema = schema;
    }

    public OAIPMHSourceDetails(String url, String schema, Set<String> sets, Date dateFrom, Date dateUntil) {
        this(url, schema);
        this.sets = sets;
        this.dateFrom = dateFrom;
        this.dateUntil = dateUntil;
    }

    public OAIPMHSourceDetails(String url, String schema, Set<String> sets, Set<String> excludeSets, Date dateFrom, Date dateUntil) {
        this(url, schema, sets, dateFrom, dateUntil);
        this.excludeSets = excludeSets;
    }

    public String getUrl() {
        return url;
    }

    public String getSchema() {
        return schema;
    }

    public Set<String> getSets() {
        return sets;
    }

    public String getSet() {
        if (sets == null || sets.isEmpty()) {
            return null;
        }
        return sets.iterator().next();
    }

    public void setSets(Set<String> sets) {
        this.sets = sets;
    }

    public Set<String> getExcludedSets() {
        return excludeSets;
    }

    public void setExcludedSets(Set<String> sets) {
        this.excludeSets = sets;
    }

    public Date getDateFrom() {
        return dateFrom;
    }

    public void setDateFrom(Date dateFrom) {
        this.dateFrom = dateFrom;
    }

    public Date getDateUntil() {
        return dateUntil;
    }

    public void setDateUntil(Date dateUntil) {
        this.dateUntil = dateUntil;
    }
}
