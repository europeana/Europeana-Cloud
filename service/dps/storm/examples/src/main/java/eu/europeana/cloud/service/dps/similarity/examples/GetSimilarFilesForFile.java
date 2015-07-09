package eu.europeana.cloud.service.dps.similarity.examples;

import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.SearchHit;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import eu.europeana.cloud.service.dps.similarity.SimilarityService;
import java.util.List;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class GetSimilarFilesForFile 
{
    private static final String[] indexers= {"elasticsearch_indexer", "solr_indexer"};
    
    private static final String file = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/K2W6FXBE64HF33WHMZNN47L5UEPWHBIRGAAUAOQRQPZ5K5UVGEAA/representations/pdf/versions/0182db50-039d-11e5-9bc7-00163eefc9c8/files/10806584_pdf_v1.pdf";
    private static final String file2 = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/5ZKCDU4N7ARXP2CM4DTQX4A2PK4FVI6FQM6ZP6ZYAT36FI6NBGMA/representations/pdf/versions/b08f9c40-1bf4-11e5-b855-00163eefc9c8/files/INTA_AM552137_EN.pdf";
    private static final String file3 = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/XT2U3SQGPDIRAYJ63SDBDCU5IXGYNPHSP3ZTGBBOOFGUV7MDVFFA/representations/pdf/versions/b6bce0f0-1bf4-11e5-b855-00163eefc9c8/files/Domestication.pdf";
    
    /**
     * @param args the command line arguments
     * @throws eu.europeana.cloud.service.dps.index.exception.IndexerException
     */
    public static void main(String[] args) throws IndexerException 
    {
        SimilarityService ss = new SimilarityService(indexers[0], "index_mlt_3", "mlt3", "192.168.47.129:9300");

        System.out.println("------  Similarities  ------");
        
        SearchResult similarDocuments = ss.getSimilarDocuments(file2, 4);
        for(SearchHit sh: similarDocuments.getHits())
        {
            System.out.print(sh.getScore());
            System.out.println("  ->  "+sh.getId());
        }
        
        System.out.println("------  Duplicates  ------");
        
        List<SearchHit> duplicateDocuments = ss.calcDuplicateDocuments(file2);
        
        for(SearchHit sh: duplicateDocuments)
        {
            System.out.print(sh.getScore());
            System.out.println("  ->  "+sh.getId());
        }
    }   
}
