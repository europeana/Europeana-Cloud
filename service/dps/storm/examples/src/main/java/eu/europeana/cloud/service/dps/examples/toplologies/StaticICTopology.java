/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;


/**
 * Example ecloud topology:
 * <p/>
 * - Creates a DpsTask using {@link StaticDpsTaskSpout}
 * <p/>
 * - Reads a File from eCloud
 * <p/>
 * - Writes a File to eCloud
 */
public class StaticICTopology {

    public static void main(String[] args) throws Exception {
        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDPsTaskForIC());
        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(staticDpsTaskSpout, new IcBolt(), MCS_URL);
        Config conf = new Config();
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, true);

        conf.put(INPUT_ZOOKEEPER_ADDRESS,
                INPUT_ZOOKEEPER_PORT);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS,
                Arrays.asList(STORM_ZOOKEEPER_ADDRESS));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, stormTopology);
        Utils.sleep(60000);
        cluster.killTopology("test");
        cluster.shutdown();

    }
}



