package eu.europeana.cloud.service.dps.service.utils.indexing;

import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.web.common.properties.IndexingProperties;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps operations on the index
 */
public class IndexWrapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      IndexWrapper.class);
  private static IndexWrapper instance;
  protected final IndexingProperties previewIndexingProperties;
  protected final IndexingProperties publishIndexingProperties;
  protected final Map<TargetIndexingDatabase, Indexer> indexers = new EnumMap<>(TargetIndexingDatabase.class);

  public IndexWrapper(IndexingProperties previewIndexingProperties,
      IndexingProperties publishIndexingProperties) {
    this.previewIndexingProperties = previewIndexingProperties;
    this.publishIndexingProperties = publishIndexingProperties;
    try {
      prepareIndexers();
    } catch (IndexingException | URISyntaxException e) {
      throw new IndexWrapperException("Unable to load indexers", e);
    }
  }

  public IndexWrapper(Properties properties) {
    previewIndexingProperties = IndexingPropertiesTransformer.getIndexingPropertiesFromPropertyFile(properties,
        IndexingType.PREVIEW);
    publishIndexingProperties = IndexingPropertiesTransformer.getIndexingPropertiesFromPropertyFile(properties,
        IndexingType.PUBLISH);
  }

  public static synchronized IndexWrapper getInstance(IndexingProperties previewIndexingProperties,
      IndexingProperties publishIndexingProperties) {
    if (instance == null) {
      instance = new IndexWrapper(previewIndexingProperties, publishIndexingProperties);
    }
    return instance;
  }

  public static synchronized IndexWrapper getInstance(Properties indexingPropertyFile) {
    if (instance == null) {
      IndexingProperties previewIndexingProperties
          = IndexingPropertiesTransformer.getIndexingPropertiesFromPropertyFile(indexingPropertyFile, IndexingType.PREVIEW);
      IndexingProperties publishIndexingProperties
          = IndexingPropertiesTransformer.getIndexingPropertiesFromPropertyFile(indexingPropertyFile, IndexingType.PUBLISH);
      instance = new IndexWrapper(previewIndexingProperties, publishIndexingProperties);
    }
    return instance;
  }


  protected enum IndexingType {
    PUBLISH,
    PREVIEW
  }

  protected void prepareIndexers() throws IndexingException, URISyntaxException {
    IndexingSettings indexingSettings;
    IndexingSettingsGenerator indexingSettingsGenerator;

    indexingSettingsGenerator = new IndexingSettingsGenerator(previewIndexingProperties, publishIndexingProperties);

    indexingSettings = indexingSettingsGenerator.generateForPreview();
    indexers.put(TargetIndexingDatabase.PREVIEW, new IndexerFactory(indexingSettings).getIndexer());
    indexingSettings = indexingSettingsGenerator.generateForPublish();
    indexers.put(TargetIndexingDatabase.PUBLISH, new IndexerFactory(indexingSettings).getIndexer());
  }

  @PreDestroy
  private void close() {
    indexers.values().forEach(indexer -> {
      try {
        indexer.close();
      } catch (IOException e) {
        LOGGER.error("Unable to close indexer", e);
      }
    });
  }

  public Indexer getIndexer(TargetIndexingDatabase targetIndexingDatabase) {
    return indexers.get(targetIndexingDatabase);
  }
}
