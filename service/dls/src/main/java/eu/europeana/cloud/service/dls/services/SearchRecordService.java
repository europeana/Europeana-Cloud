package eu.europeana.cloud.service.dls.services;

import java.util.List;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.dls.solr.RepresentationSearchParams;
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
     * Searches for representation versions by multiple parameters. Result is returned in slices which contain fixed
     * amount of results and reference (token) to next slice of results.
     * @param searchParams parameters 
     * @param thresholdParam Threshold param to indicate result slice
     * @param limit maximum number of results to return
     * @return slice of representations matching parameters and token to the next page of results
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
