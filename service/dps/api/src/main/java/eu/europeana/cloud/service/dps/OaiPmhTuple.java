package eu.europeana.cloud.service.dps;

import java.util.Date;
import java.util.Set;

/**
 * @author akrystian.
 */
public class OaiPmhTuple {
    Set<String> schemas;
    Set<String> excludedSchemas;
    Set<String> sets;
    Set<String> excludedSets;
    Date from;
    Date until;

    public OaiPmhTuple() {
    }

    public OaiPmhTuple(Set<String> schemas, Set<String> excludedSchemas, Set<String> sets, Set<String> excludedSets, Date from, Date until) {
        this.schemas = schemas;
        this.excludedSchemas = excludedSchemas;
        this.sets = sets;
        this.excludedSets = excludedSets;
        this.from = from;
        this.until = until;
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

    public Set<String> getExcludedSets() {
        return excludedSets;
    }

    public Date getFrom() {
        return from;
    }

    public Date getUntil() {
        return until;
    }
}
