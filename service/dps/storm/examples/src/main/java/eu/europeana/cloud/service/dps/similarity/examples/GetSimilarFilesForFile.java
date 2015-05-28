package eu.europeana.cloud.service.dps.similarity.examples;

import eu.europeana.cloud.service.dps.index.exception.IndexException;
import eu.europeana.cloud.service.dps.similarity.SimilarDocument;
import eu.europeana.cloud.service.dps.similarity.SimilarityService;
import java.util.List;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class GetSimilarFilesForFile 
{
    private static final String file = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/K2W6FXBE64HF33WHMZNN47L5UEPWHBIRGAAUAOQRQPZ5K5UVGEAA/representations/pdf/versions/0182db50-039d-11e5-9bc7-00163eefc9c8/files/10806584_pdf_v1.pdf";
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IndexException 
    {
        SimilarityService ss = new SimilarityService("192.168.47.129:9300", "test_index1", "test_type1");
        List<SimilarDocument> similarDocuments = ss.getSimilarDocuments_naiveImplementation(file, 4);
        
        for(SimilarDocument sd: similarDocuments)
        {
            System.err.println(sd.getAssignedDocument() + " #score: "+ sd.getScore());
        }
        
        System.err.println("-------------");
        
        List<String> duplicateDocuments = ss.calcDuplicateDocuments_naiveImplementation(file);
        
        for(String s: duplicateDocuments)
        {
            System.err.println(s);
        }
    }
    
}
