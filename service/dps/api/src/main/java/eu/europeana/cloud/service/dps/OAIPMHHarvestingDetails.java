package eu.europeana.cloud.service.dps;

import com.google.common.base.Objects;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@XmlRootElement()
public class OAIPMHHarvestingDetails implements Serializable {

    /** Schemas to harvest - optional */
    private Set<String> schemas;

    /** Schemas to exclude - optional */
    private Set<String> excludedSchemas;

    /** Sets to harvest - optional */
    private Set<String> sets;

    /** Sets to exclude - optional */
    private Set<String> excludeSets;

    /** From date - optional */
    private Date dateFrom;

    /** Until date - optional */
    private Date dateUntil;

    public OAIPMHHarvestingDetails() {
        // serialization purposes
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
    @SuppressWarnings({"squid:S1067"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OAIPMHHarvestingDetails)) {
            return false;
        }
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
