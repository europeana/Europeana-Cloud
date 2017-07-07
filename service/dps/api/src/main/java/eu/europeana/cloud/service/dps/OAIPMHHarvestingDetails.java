package eu.europeana.cloud.service.dps;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class OAIPMHHarvestingDetails implements Serializable {

    /** Schemas to harvest */
    private Set<String> schemas;

    /** Schemas to exclude */
    private Set<String> excludedSchemas;

    /** Sets to harvest */
    private Set<String> sets;

    /** Sets to exclude */
    private Set<String> excludeSets;

    /** From date */
    private Date dateFrom;

    /** Until date */
    private Date dateUntil;

    public OAIPMHHarvestingDetails() {
    }

    public OAIPMHHarvestingDetails(String schema) {
        this.schemas = new HashSet<>();
        this.schemas.add(schema);
    }

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


    public Set<String> getSchemas() {
        return schemas;
    }

    public Set<String> getExcludedSchemas() {
        return excludedSchemas;
    }

    public Set<String> getSets() {
        return sets;
    }

    public Set<String> getExcludeSets() {
        return excludeSets;
    }

    public Date getDateFrom() {
        return dateFrom;
    }

    public Date getDateUntil() {
        return dateUntil;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OAIPMHHarvestingDetails)) return false;
        OAIPMHHarvestingDetails that = (OAIPMHHarvestingDetails) o;
        return Objects.equal(schemas, that.schemas) &&
                Objects.equal(excludedSchemas, that.excludedSchemas) &&
                Objects.equal(sets, that.sets) &&
                Objects.equal(excludeSets, that.excludeSets) &&
                Objects.equal(dateFrom, that.dateFrom) &&
                Objects.equal(dateUntil, that.dateUntil);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schemas, excludedSchemas, sets, excludeSets, dateFrom, dateUntil);
    }
}
