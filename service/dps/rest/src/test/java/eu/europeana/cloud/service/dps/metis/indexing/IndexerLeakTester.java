package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.indexing.exception.IndexingException;

import static eu.europeana.cloud.service.dps.metis.indexing.DatabaseLocation.DEFAULT_PREVIEW;

public class IndexerLeakTester {


    public static void main(String[] args) throws IndexingException {
long r;
        TestIndexWrapper w = new TestIndexWrapper();
       // long r = w.indexers.get(DEFAULT_PREVIEW).countRecords("79");


        for(int i=0;i<10000;i++) {
//            TestIndexWrapper w = new TestIndexWrapper();
            r = w.indexers.get(DEFAULT_PREVIEW).countRecords("79");
            System.out.println("\n\nOK! i="+i+", size=" + r+"\n\n");
        }
    }


}
