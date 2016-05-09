/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.europeana.cloud.service.dps.index;

import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.index.structure.SearchHit;
import eu.europeana.cloud.service.dps.index.structure.SearchResult;


/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class Test 
{
    private static final String[] indexers = {"elasticsearch_indexer", "solr_indexer"};
    private static final String[] addresses = {"192.168.47.129:9300", "http://192.168.47.129:8983/solr"};

    private static final String file = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/5ZKCDU4N7ARXP2CM4DTQX4A2PK4FVI6FQM6ZP6ZYAT36FI6NBGMA/representations/pdf/versions/b08f9c40-1bf4-11e5-b855-00163eefc9c8/files/INTA_AM552137_EN.pdf";
    private static final String file2 = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/XT2U3SQGPDIRAYJ63SDBDCU5IXGYNPHSP3ZTGBBOOFGUV7MDVFFA/representations/pdf/versions/b6bce0f0-1bf4-11e5-b855-00163eefc9c8/files/Domestication.pdf";
    private static final String solrDomes = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/3YV2UXZU4WIQJCBP2SJL2DONBA4Q6STMQ5JYPXU2F7ES4XQX2C5Q/representations/pdf/versions/e333e780-16a6-11e5-b855-00163eefc9c8/files/Domestication.pdf";
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception
    {
        String theFile = solrDomes;
        
        Indexer indexer = IndexerFactory.getIndexer(new IndexerInformations(indexers[1], "index_mlt_4", "mlt4", addresses[1]));

        IndexedDocument document = indexer.getDocument(theFile);
        
        if(document == null)
        {
            System.err.println("No result!");
            return;
        }
        
        //indexer.insert(document.getId()+"_duplicate",document.getData());
        
        //----------
        
        System.err.println("\nSearch:");
        IndexFields _if[] = {IndexFields.RAW_TEXT, IndexFields.DESCRIPTION};
        SearchResult search = indexer.search("dog is domesticated animal", IndexFields.toStringArray(_if),4,6000);

        for(SearchHit sh: search.getHits())
        {
            System.err.print(sh.getScore());
            System.err.println(" -> "+sh.getId());
        }
        
        System.err.println("max score: "+search.getMaxScore());
        System.err.println("total hits: "+search.getTotalHits());
        System.err.println("time: "+search.getTookTime());
        
        System.err.println("Scroll:");
        SearchResult nextPage = indexer.getNextPage(search.getScrollId(), search);
        
        if(nextPage != null)
        {
            for(SearchHit sh: nextPage.getHits())
            {
                System.err.print(sh.getScore());
                System.err.println(" -> "+sh.getId());
            }

            System.err.println("max score: "+nextPage.getMaxScore());
            System.err.println("total hits: "+nextPage.getTotalHits());
            System.err.println("time: "+nextPage.getTookTime());
        }
        else
        {
            System.out.println("No next page.");
        }
        
        //-------------
        
        System.err.println("\nMLT:");
        SearchResult mlt = indexer.getMoreLikeThis(theFile);
        
        for(SearchHit sh: mlt.getHits())
        {
            System.err.print(sh.getScore());
            System.err.println(" -> "+sh.getId());
        }        
                
        System.err.println("max score: "+mlt.getMaxScore());
        System.err.println("total hits: "+mlt.getTotalHits());
        System.err.println("time: "+mlt.getTookTime());
        
        //--------------
        
        System.err.println("\nPhrase:");
        SearchResult phr = indexer.searchPhraseInFullText("dog is animal", 4);
        
        for(SearchHit sh: phr.getHits())
        {
            System.err.print(sh.getScore());
            System.err.println(" -> "+sh.getId());
        }
                
        System.err.println("max score: "+phr.getMaxScore());
        System.err.println("total hits: "+phr.getTotalHits());
        System.err.println("time: "+phr.getTookTime());
        
        
        //--------------
        System.err.println("\nAdvanced search:");
        SearchResult aSearch = indexer.advancedSearch("raw_text:domestication^3", 4, Indexer.TIMEOUT);
        
        for(SearchHit sh: aSearch.getHits())
        {
            System.err.print(sh.getScore());
            System.err.println(" -> "+sh.getId());
        }
                
        System.err.println("max score: "+aSearch.getMaxScore());
        System.err.println("total hits: "+aSearch.getTotalHits());
        System.err.println("time: "+aSearch.getTookTime());
    }   
}
