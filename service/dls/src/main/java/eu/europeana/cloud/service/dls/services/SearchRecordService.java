package eu.europeana.cloud.service.dls.services;

import java.util.List;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.dls.RepresentationSearchParams;
import eu.europeana.cloud.service.dls.solr.SolrDAO;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Search service using Solr as storage.
 */
@Service
public class SearchRecordService {

    @Autowired
    private SolrDAO solrDAO;

    /**
     * Searches for specified representations and returns result in slices.
     * 
     * @param searchParams
     *            search parameters.
     * @param thresholdParam
     *            if null - will return first result slice. Result slices
     *            contain token for next pages, which should be provided in this
     *            parameter for subsequent result slices.
     * @param limit
     *            max number of results in one slice.
     * @return found representations.
     */
    public ResultSlice<Representation> search(RepresentationSearchParams searchParams, String thresholdParam, int limit) {
        int startFrom = 0;
        if (thresholdParam != null) {
            try {
                startFrom = Integer.parseInt(thresholdParam);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Wrong threshold param for searching");
            }
        }
        List<Representation> foundRepresenations = solrDAO.search(searchParams, startFrom, limit + 1);
        String nextResultToken = null;
        if (foundRepresenations.size() == limit + 1) {
            nextResultToken = Integer.toString(startFrom + limit + 1);
            foundRepresenations.remove(limit);
        }
        return new ResultSlice<>(nextResultToken, foundRepresenations);
    }
}
