package eu.europeana.cloud.common.selectors;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;

import java.util.List;

/**
 * Selects one representation from provided list of representations according to implementation.
 * 
 */
public interface RepresentationSelector {
    
    Representation select(List<Representation> representations);
}
