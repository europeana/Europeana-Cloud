package eu.europeana.cloud.service.dps;

import java.util.Date;
import java.util.Set;

public class OAIPMHHarvestingDetails {

    /** Schemas to harvest */
    private Set<String> schemas = null;

    /** Schemas to exclude */
    private Set<String> excludedSchemas = null;

    /** Sets to harvest */
    private Set<String> sets = null;

    /** Sets to exclude */
    private Set<String> excludeSets = null;

    /** From date */
    private Date dateFrom = null;

    /** Until date */
    private Date dateUntil = null;

    public OAIPMHHarvestingDetails(Set<String> schemas, Set<String> sets, Date dateFrom, Date dateUntil) {
        this.schemas = schemas;
        this.sets = sets;
        this.dateFrom = dateFrom;
        this.dateUntil = dateUntil;
    }

    public OAIPMHHarvestingDetails(Set<String> schemas, Set<String> excludedSchemas, Set<String> sets, Set<String> excludeSets, Date dateFrom, Date dateUntil) {
        this(schemas, sets, dateFrom, dateUntil);
        this.excludedSchemas = excludedSchemas;
        this.excludeSets = excludeSets;
    }

    public String getSchema() {
        if (schemas == null || schemas.isEmpty()) {
            return null;
        }
        return schemas.iterator().next();
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

    public Set<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    public Set<String> getExcludedSchemas() {
        return excludedSchemas;
    }

    public void setExcludedSchemas(Set<String> excludedSchemas) {
        this.excludedSchemas = excludedSchemas;
    }

    public Set<String> getExcludeSets() {
        return excludeSets;
    }

    public void setExcludeSets(Set<String> excludeSets) {
        this.excludeSets = excludeSets;
    }
}
