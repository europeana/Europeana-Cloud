package eu.europeana.cloud.service.mcs.utils;

import org.apache.log4j.BasicConfigurator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Removes all records from Metadata & Content service.
 */
public final class MCSCleaner {

    private static final int STATUS_CODE_NO_CONTENT = 204;
    private static final int FACET_LIMIT = 100;
    private static Logger logger = LoggerFactory.getLogger(MCSCleaner.class);
    private static Client client = JerseyClientBuilder.newClient();


    private MCSCleaner() {
        // Utility classes should not have a public or default constructor
    }


    /**
     * Removes all records from Metadata & Content service. Get all record identifiers from Solr, then send DELETE/{id}
     * request to MCS.
     *
     * @param args URL to Solr server, URL to Metadata & Content service.
     */
    public static void main(String[] args) {

        BasicConfigurator.configure();
        List<String> idList = new ArrayList<>();

        if (args.length < 2) {
            throw new IllegalStateException("SolrURL & MCSUrl prooperties are required");
        }

        String solrUrl = args[0];
        String mcsUrl = args[1];

        SolrClient solrServer = new HttpSolrClient(solrUrl);
        try {
            SolrQuery query = new SolrQuery("*:*").addFacetField("cloud_id").setRows(0).setFacet(true)
                    .setFacetLimit(FACET_LIMIT);

            int resultCount = 1;
            int offset = 0;
            while (resultCount != 0) {
                query.set("facet.offset", offset);

                QueryResponse response = solrServer.query(query);

                FacetField facets = response.getFacetField("cloud_id");
                resultCount = facets.getValueCount();
                offset += resultCount;

                List<FacetField.Count> values = facets.getValues();
                for (FacetField.Count field : values) {
                    idList.add(field.getName());
                }
            }
            logger.info("{} records to delete", idList.size());
            for (String id : idList) {
                logger.debug("Deleting {}", id);
                Response response = client.target(mcsUrl + "records/" + id).request().delete();
                if (response.getStatus() != STATUS_CODE_NO_CONTENT) {
                    logger.error("Cannot remove record {} {}", id, response.toString());
                }
            }
        } catch (SolrServerException | IOException ex) {
            logger.error(ex.getMessage());
        } finally {
            try {
                solrServer.close();
            } catch (IOException ex) {
                logger.error("Exception while closing solr {}",ex.getMessage());
            }
        }
    }
}
