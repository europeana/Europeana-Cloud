package eu.europeana.cloud.service.mcs.cleaner;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.client.Client;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.glassfish.jersey.client.JerseyClientBuilder;

/**
 * Removes all records from Metadata & Content service.
 * 
 */
public class MCSCleaner {

    private static Logger logger = Logger.getLogger(MCSCleaner.class);
    private static Client client = JerseyClientBuilder.newClient();


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

        System.out.println(solrUrl);
        System.out.println(mcsUrl);

        SolrServer solrServer = new HttpSolrServer(solrUrl);
        try {
            SolrQuery query = new SolrQuery("*:*");
            query.addFacetField("cloud_id").setRows(0);
            FacetField facets = solrServer.query(query).getFacetField("cloud_id");
            List<FacetField.Count> values = facets.getValues();
            for (FacetField.Count field : values) {
                idList.add(field.getName());
            }

            for (String id : idList) {
                logger.info("Deleting " + id);
                client.target(mcsUrl + id).request().delete();
            }
        } catch (SolrServerException ex) {
            logger.error(ex);
        } finally {
            solrServer.shutdown();
        }
    }
}
