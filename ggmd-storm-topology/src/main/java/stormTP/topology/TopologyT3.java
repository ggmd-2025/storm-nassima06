package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.Exit3Bolt;
import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;

/**
 * Topologie T3 : 
 * InputStreamSpout -> GiveRankBolt -> Exit3Bolt
 */
public class TopologyT3 {
    
    public static void main(String[] args) throws Exception {
        int nbExecutors = 1;
        int portINPUT = Integer.parseInt(args[0]);
        int portOUTPUT = Integer.parseInt(args[1]);
        
        // Création du spout
        InputStreamSpout spout = new InputStreamSpout("127.0.0.1", portINPUT);

        // Création de la topologie
        TopologyBuilder builder = new TopologyBuilder();

        // Spout source
        builder.setSpout("masterStream", spout);

        // Bolt de classement : GiveRankBolt, en entrée du spout
        builder.setBolt("giverank", new GiveRankBolt(), nbExecutors)
               .shuffleGrouping("masterStream");

        // Bolt de sortie : Exit3Bolt, en entrée de GiveRankBolt
        builder.setBolt("exit3", new Exit3Bolt(portOUTPUT), nbExecutors)
               .shuffleGrouping("giverank");
       
        // Configuration
        Config config = new Config();

        // Soumission de la topologie
        StormSubmitter.submitTopology("topoT3", config, builder.createTopology());
    }
}
