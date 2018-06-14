package eu.europeana.cloud.service.dps.storm.topologies.indexer;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.index.Indexer;
import eu.europeana.cloud.service.dps.index.IndexerFactory;
import eu.europeana.cloud.service.dps.index.SupportedIndexers;
import eu.europeana.cloud.service.dps.index.exception.IndexerException;
import eu.europeana.cloud.service.dps.index.structure.IndexedDocument;
import eu.europeana.cloud.service.dps.index.structure.IndexerInformations;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.apache.commons.lang.NotImplementedException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;


/**
 * Marge more indexed documents to one new document.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class MergeIndexedDocumentsBolt extends AbstractDpsBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeIndexedDocumentsBolt.class);

    private final Map<SupportedIndexers, String> clastersAddresses;
    private final int cacheSize;

    private transient LRUCache<String, Indexer> clients;

    /**
     * Constructor of MergeIndexedDocumentsBolt.
     *
     * @param clastersAddresses map of indexers and their connection strings
     * @param cacheSize         number of established connection in cache.
     */
    public MergeIndexedDocumentsBolt(Map<SupportedIndexers, String> clastersAddresses, int cacheSize) {
        this.clastersAddresses = clastersAddresses;
        this.cacheSize = cacheSize;
    }

    @Override
    public void execute(StormTaskTuple t) {
        Indexer indexer = getIndexer(t.getParameter(PluginParameterKeys.INDEXER));

        if (indexer == null) {
            LOGGER.warn("No indexer. Task {} is dropped.", t.getTaskId());
            emitDropNotification(t.getTaskId(), t.getFileUrl(), "No indexer.", t.getParameters().toString());
            //endTask(t.getTaskId(), "No indexer. Task " + t.getTaskId() + " is dropped.", TaskState.DROPPED, new Date());
            outputCollector.ack(inputTuple);
            return;
        }

        List<String> docIds = getDocumentIdsFromAnnotation(t.getFileUrl());
        Map<String, Object> mergedData = new HashMap<>();

        for (String docId : docIds) {
            try {
                //retrieve data from index
                IndexedDocument document = indexer.getDocument(docId);
                Map<String, Object> data = document.getData();

                //merge data
                for (Map.Entry<String, Object> d : data.entrySet()) {
                    String key = d.getKey();

                    if (mergedData.containsKey(key))     //key already exists => conflict
                    {
                        Object val = d.getValue();
                        Object mData = mergedData.get(key);

                        if (val instanceof Collection<?>)    //new value is list
                        {
                            if (mData instanceof Collection<?>)  //marged data are list as well
                            {
                                Collection<Object> tmp = (Collection) mData;
                                tmp.addAll((Collection) val);
                            } else {
                                Collection<Object> tmp = (Collection) val;
                                tmp.add(mData);

                                mergedData.put(key, tmp);
                            }
                        } else if (mData instanceof Collection<?>)    //marged data already contains list
                        {
                            Collection<Object> tmp = (Collection) mData;
                            tmp.add(val);
                        } else    //put list to margedData
                        {
                            List<Object> l = new ArrayList<>();
                            l.add(mData);
                            l.add(val);

                            mergedData.put(key, l);
                        }
                    } else {
                        mergedData.put(d.getKey(), d.getValue());
                    }
                }
            } catch (IndexerException ex) {
                LOGGER.warn("Cannot read indexed document because: " + ex.getMessage());
            }
        }

        try {
            t.setFileData(new ObjectMapper().writeValueAsBytes(mergedData));
        } catch (IOException ex) {
            LOGGER.warn("Cannot serialize merged data because: " + ex.getMessage());
            StringWriter stack = new StringWriter();
            ex.printStackTrace(new PrintWriter(stack));
            emitDropNotification(t.getTaskId(), t.getFileUrl(), "Cannot serialize merged data.", stack.toString());
            //endTask(t.getTaskId(), ex.getMessage(), TaskState.DROPPED, new Date());
            outputCollector.ack(inputTuple);
            return;
        }

        LOGGER.info("Merged documents: {}", docIds);

        outputCollector.emit(inputTuple, t.toStormTuple());
        outputCollector.ack(inputTuple);
    }

    @Override
    public void prepare() {
        clients = new LRUCache<>(cacheSize);
    }

    private Indexer getIndexer(String data) {
        IndexerInformations ii = IndexerInformations.fromTaskString(data);

        if (ii == null) {
            return null;
        }

        String key = ii.toKey();
        if (clients.containsKey(key)) {
            return clients.get(key);
        }

        //key not exists => open new connection and add it to cache

        ii.setAddresses(clastersAddresses.get(ii.getIndexerName()));

        Indexer client = IndexerFactory.getIndexer(ii);
        clients.put(key, client);

        return client;
    }

    private List<String> getDocumentIdsFromAnnotation(String annotationUrl) {
        //TODO: retrieve IDs from annotation
        throw new NotImplementedException("Waiting for Annotation service!");
    }
}