package eu.europeana.cloud.normalization.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;

/**
 * Call the remote normalization service in order to normalize and clean an edm record.
 * <p/>
 * Receives a byte array representing a Record from a tuple, normalizes its content nad stores it as part of the emitted tuple.
 */
public class NormalizationBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationBolt.class);
  private static final String NORMALIZATION_EX_MESSAGE = "Unable to normalize file";

  private transient NormalizerFactory normalizerFactory;

  public NormalizationBolt(CassandraProperties cassandraProperties) {
    super(cassandraProperties);
  }

  /**
   * Prepare the bolt for execution. Initializes NormalizerFactory.
   */
  @Override
  public void prepare() {
    normalizerFactory = new NormalizerFactory();
  }

  /**
   * Retrieves the edm record from stormTaskTuple. Executes normalization using Metis Normalization library and receives a
   * normalized and cleaned EDM record as the result. The result is emitted as a tuple. In case of an exception error notification
   * is emitted.
   *
   * @param stormTaskTuple tuple containing input data
   */
  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    try {
      final Normalizer normalizer = normalizerFactory.getNormalizer();
      String document = new String(stormTaskTuple.getFileData(), StandardCharsets.UTF_8);

      NormalizationResult normalizationResult = normalizer.normalize(document);

      if (normalizationResult.getErrorMessage() != null) {
        LOGGER.error(NORMALIZATION_EX_MESSAGE, normalizationResult.getErrorMessage());
          emitErrorNotification(anchorTuple, stormTaskTuple, normalizationResult.getErrorMessage(), "Error during normalization.");
      } else {
        String output = normalizationResult.getNormalizedRecordInEdmXml();
        emitNormalizedContent(anchorTuple, stormTaskTuple, output);
      }
    } catch (NormalizationConfigurationException e) {
        LOGGER.error(NORMALIZATION_EX_MESSAGE, e);
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(),
                "Error in normalizer configuration. The full error is: " + ExceptionUtils.getStackTrace(e));
    } catch (NormalizationException e) {
        LOGGER.error(NORMALIZATION_EX_MESSAGE, e);
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(),
                "Error during normalization. The full error is: " + ExceptionUtils.getStackTrace(e));
    } catch (MalformedURLException e) {
        LOGGER.error(NORMALIZATION_EX_MESSAGE, e);
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(),
                "Cannot prepare output storm tuple. The full error is: " + ExceptionUtils.getStackTrace(e));
    }
    outputCollector.ack(anchorTuple);
  }

  private void emitNormalizedContent(Tuple anchorTuple, StormTaskTuple stormTaskTuple, String output)
      throws MalformedURLException {
    prepareStormTaskTupleForEmission(stormTaskTuple, output);
    outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
  }
}
