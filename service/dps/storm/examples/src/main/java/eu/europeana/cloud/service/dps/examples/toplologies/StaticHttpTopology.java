package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.http.HTTPHarvestingTopology;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.HTTP_TOPOLOGY;

public class StaticHttpTopology {
    private static final Logger LOGGER = LoggerFactory.getLogger(StaticHttpTopology.class);

    public static void main(String[] args) {
        String providedPropertyFile = null;
        if(args.length > 0) {
            File propertyFile = new File(args[0]);
            if(propertyFile.exists() && !propertyFile.isDirectory()) {
                providedPropertyFile = propertyFile.getAbsolutePath();
            } else {
                LOGGER.warn("Inavalid proprty file: '{}'. Only default properties will be used.", args[0]);
            }
        }

        HTTPHarvestingTopology httpHarvestingTopology =
                new HTTPHarvestingTopology("http-topology-config.properties", providedPropertyFile);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(HTTP_TOPOLOGY,
                TopologyHelper.buildConfig(HTTPHarvestingTopology.getProperties(), true),
                httpHarvestingTopology.buildTopology());

        Utils.sleep(1000L*60*1000); //1000 minutes
        cluster.killTopology(HTTP_TOPOLOGY);
        cluster.shutdown();
    }
}
