package eu.europeana.cloud.service.mcs.cleaner;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import org.apache.log4j.BasicConfigurator;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes all records from Metadata & Content service.
 * 
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
     * @param args
     *            URL to Solr server, URL to Metadata & Content service.
     */
    public static void main(String[] args) {

        BasicConfigurator.configure();
        List<String> idList = new ArrayList<>();

        if (args.length < 2) {
            throw new IllegalStateException("SolrURL & MCSUrl prooperties are required");
        }

        String solrUrl = args[0];
        String mcsUrl = args[1];

        SolrServer solrServer = new HttpSolrServer(solrUrl);
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
            logger.info(idList.size() + " records to delete");
            for (String id : idList) {
                logger.debug("Deleting " + id);
                Response response = client.target(mcsUrl + "records/" + id).request().delete();
                if (response.getStatus() != STATUS_CODE_NO_CONTENT) {
                    logger.error("Cannot remove record " + id + " " + response.toString());
                }
            }
        } catch (SolrServerException ex) {
            logger.error(ex.getMessage());
        } finally {
            solrServer.shutdown();
        }
    }
}
