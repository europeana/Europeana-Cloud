package eu.europeana.cloud.service.dps.similarity;

import eu.europeana.cloud.service.dps.index.Elasticsearch;
import eu.europeana.cloud.service.dps.index.exception.IndexException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;
import eu.europeana.cloud.service.dps.storm.topologies.indexer.IndexerConstants;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SimilarityService 
{
    private final float duplicationThreshold = (float)0.98; //1 = 100%
    private final Elasticsearch client;
    
    public SimilarityService(String addresses, String index, String type) throws IndexException 
    {  
        client = new Elasticsearch(addresses, index, type);       
    }
    
    public List<SimilarDocument> getSimilarDocuments_naiveImplementation(String documentUrl, int limit)
    {
        IndexedDocument document = client.getDocument(documentUrl);
        
        List<SearchResult> result = client.simpleMatchQuery(IndexerConstants.RAW_DATA_FIELD, 
                (String)document.getData().get(IndexerConstants.RAW_DATA_FIELD), limit+1);   //+1 because it returns source document as well
        
        result.remove(0);   //remove the source document
        
        List<SimilarDocument> res = new ArrayList();
        for(SearchResult sr: result)
        {         
            SimilarDocument sd = new SimilarDocument(documentUrl, sr.getId());
            sd.setScore(sr.getScore());
            res.add(sd);
        }
        
        return res;
    }
    
    public List<String> calcDuplicateDocuments_naiveImplementation(String documentUrl)
    {
        IndexedDocument document = client.getDocument(documentUrl);
        
        List<SearchResult> result = client.simpleMatchQuery(IndexerConstants.RAW_DATA_FIELD, 
                (String)document.getData().get(IndexerConstants.RAW_DATA_FIELD), duplicationThreshold);
        
        SearchResult sourceDocument = result.remove(0); //remove the source document
        float threshold = sourceDocument.getScore() * duplicationThreshold;     //score of source document not 1 but more

        List<String> res = new ArrayList();
        for(SearchResult sr: result)
        {
            if(sr.getScore() < threshold)
            {
                break;
            }
            res.add(sr.getId());
        }
        
        return res;
    }
}
