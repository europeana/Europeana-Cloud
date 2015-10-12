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
package eu.europeana.cloud.service.dps.xslt.kafka.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;


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

    private static String ecloudMcsAddress = "http://iks-kbase.synat.pcss.pl:9090/mcs";
    private static String username = "admin";
    private static String password = "admin";

    //private static String zkAddress = "admin";
    //private static String zkAddress = "admin";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        StaticDpsTaskSpout taskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDPsTaskForIC());
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress, username, password);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress, username, password);
        builder.setSpout("taskSpout", taskSpout, 1);
        builder.setBolt("retrieveFileBolt", retrieveFileBolt, 1).shuffleGrouping(
                "taskSpout");
        builder.setBolt("imageConversionBolt", new IcBolt(), 1).shuffleGrouping(
                "retrieveFileBolt");
        builder.setBolt("writeRecordBolt", writeRecordBolt, 1).shuffleGrouping(
                "imageConversionBolt");
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                    builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(60000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}



