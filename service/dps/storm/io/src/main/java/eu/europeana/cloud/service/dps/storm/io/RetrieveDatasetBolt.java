package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author la4227 <lucas.anastasiou@open.ac.uk>
 */
public class RetrieveDatasetBolt extends AbstractDpsBolt {

    private OutputCollector collector;

    private static final Logger LOGGER = LoggerFactory
            .getLogger(RetrieveDatasetBolt.class);

    /**
     * Properties to connect to eCloud
     */
    private final String zkAddress;
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;

    private FileServiceClient fileClient;
    private DataSetServiceClient datasetClient;

    public RetrieveDatasetBolt(String zkAddress, String ecloudMcsAddress, String username,
            String password) {

        this.zkAddress = zkAddress;
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {

        this.collector = collector;
        datasetClient = new DataSetServiceClient(ecloudMcsAddress, username, password);
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
    }

    @Override
    public void execute(StormTaskTuple t) {
        System.out.println("i received a stormTaskTuple ");
    }

    @Override
    public void execute(Tuple t) {
        // expects a tuple of two fields: providerId,datasetId
        //
        try {
            System.out.println("i received a normal tuple");
            System.out.println(t.toString());
            String providerId = t.getStringByField("providerId");
            String datasetId = t.getStringByField("datasetId");
            System.out.println("providerId = " + providerId);
            System.out.println("datasetId = " + datasetId);

            //
            // fetch all represenations of dataset
            //
            List<Representation> representations = datasetClient.getDataSetRepresentations(providerId, datasetId);

            for (Representation representation : representations) {
                System.out.println("rep : " + representation.toString());
                StormTaskTuple stt = new StormTaskTuple();
                File file = representation.getFiles().get(0);

                if (file == null) {
                    System.out.println("file is null, continuing ");
                    continue;
                }

                System.out.println(file.toString());

//                URI uri = file.getContentUri();
//                if (uri == null) {
                // uri is most of the cases null, why?
//                    System.out.println("uri is null, continuing");
//                    continue;
//                }
                //
                // read file
                //
                InputStream is = fileClient.getFile(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName());

                String data;
                try {
                    data = IOUtils.toString(is);

                    stt.setFileData(data);

                    // it would be good to send only the file uri instead of the whole file as a string
                    // but uri is null (taken from the representation), why?
//                System.out.println("setting uri string:" + uri.toString());
//                stt.setFileUrl(uri.toString());
                    System.out.println("emitting...");
                    collector.emit(stt.toStormTuple());
                } catch (IOException ex) {
                    java.util.logging.Logger.getLogger(RetrieveDatasetBolt.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        } catch (MCSException ex) {
            LOGGER.error(ex.getMessage());
        }

    }

}
