package eu.europeana.cloud.service.dps;

import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonIgnore;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@XmlRootElement()
public class OAIPMHHarvestingDetails implements Serializable {

    /**
     * Schemas to harvest - optional
     */
    private Set<String> schemas;

    /**
     * Schemas to exclude - optional
     */
    private Set<String> excludedSchemas;

    /**
     * Sets to harvest - optional
     */
    private Set<String> sets;

    /**
     * Sets to exclude - optional
     */
    private Set<String> excludedSets;

    /**
     * From date - optional
     */
    private Date dateFrom;

    /**
     * Until date - optional
     */
    private Date dateUntil;

    /**
     * dates granularity supported by the source
     */
    private String granularity;

    public OAIPMHHarvestingDetails() {
        // serialization purposes
    }

    public OAIPMHHarvestingDetails(String schema) {
        this.schemas = new HashSet<>();
        this.schemas.add(schema);
    }

    public OAIPMHHarvestingDetails(Set<String> schemas, Set<String> sets, Date dateFrom, Date dateUntil, String granularity) {
        this.schemas = schemas;
        this.sets = sets;
        this.dateFrom = dateFrom;
        this.dateUntil = dateUntil;
        this.granularity = granularity;
    }

    public OAIPMHHarvestingDetails(Set<String> schemas, Set<String> excludedSchemas, Set<String> sets, Set<String> excludeSets, Date dateFrom, Date dateUntil, String granularity) {
        this(schemas, sets, dateFrom, dateUntil, granularity);
        this.excludedSchemas = excludedSchemas;
        this.excludedSets = excludeSets;
    }

    @JsonIgnore
    public String getSchema() {
        if (schemas == null || schemas.isEmpty()) {
            return null;
        }
        return schemas.iterator().next();
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public Set<String> getExcludedSchemas() {
        return excludedSchemas;
    }

    @JsonIgnore
    public String getSet() {
        if (sets == null || sets.isEmpty()) {
            return null;
        }
        return sets.iterator().next();
    }

    public Set<String> getSets() {
        return sets;
    }

    public Set<String> getExcludedSets() {
        return excludedSets;
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

    public String getGranularity() { return granularity; }

    public void setDateUntil(Date dateUntil) {
        this.dateUntil = dateUntil;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    public void setExcludedSchemas(Set<String> excludedSchemas) {
        this.excludedSchemas = excludedSchemas;
    }

    public void setSets(Set<String> sets) {
        this.sets = sets;
    }

    public void setExcludedSets(Set<String> excludeSets) {
        this.excludedSets = excludeSets;
    }

    public void setGranularity(String granularity) {
        this.granularity = granularity;
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
                Objects.equal(excludedSets, that.excludedSets) &&
                Objects.equal(dateFrom, that.dateFrom) &&
                Objects.equal(dateUntil, that.dateUntil);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schemas, excludedSchemas, sets, excludedSets, dateFrom, dateUntil, granularity);
    }

    @Override
    public String toString() {
        return "OAIPMHHarvestingDetails{" +
                "schemas=" + schemas +
                ", excludedSchemas=" + excludedSchemas +
                ", sets=" + sets +
                ", excludedSets=" + excludedSets +
                ", dateFrom=" + dateFrom +
                ", dateUntil=" + dateUntil +
                ", granularity=" + granularity +
                '}';
    }


}
