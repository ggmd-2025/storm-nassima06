package stormTP.operator;

import java.util.Map;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import javax.json.JsonArray;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * Sample of stateless operator
 * @author lumineau
 *
 */
public class GiveRankBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("GiveRankBoltLogger");
	//Attributs de GiveRankBolt
    private OutputCollector collector;
  
    public GiveRankBolt (){
       
    }

    public void prepare(Map arg1, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
    }


    public void execute(Tuple t) {
        try {
            // Get JSON string from input tuple
            String jsont = t.getStringByField("json");

            // Parse JSON
            JsonReader reader = Json.createReader(new StringReader(jsont));
            JsonObject jsonObject = reader.readObject();
            JsonArray runners = jsonObject.getJsonArray("runners");

            int n = runners.size();
            if (n == 0) {
                collector.ack(t);
                return;
            }

            int[] ids = new int[n];
            int[] tops = new int[n];
            int[] totals = new int[n];
            int[] maxcels = new int[n];
            long[] distances = new long[n];

            // First pass: extract data and compute distance for each runner
            for (int i = 0; i < n; i++) {
                JsonObject runner = runners.getJsonObject(i);

                int id = runner.getInt("id");
                int top = runner.getInt("top");
                int tour = runner.getInt("tour");
                int cellule = runner.getInt("cellule");
                int maxcel = runner.getInt("maxcel");
                int total = runner.getInt("total");

                long distance = (long) tour * maxcel + cellule;

                ids[i] = id;
                tops[i] = top;
                totals[i] = total;
                maxcels[i] = maxcel;
                distances[i] = distance;
            }

            // Second pass: compute rank for each runner
            int[] baseRanks = new int[n];
            for (int i = 0; i < n; i++) {
                int rank = 1;
                for (int j = 0; j < n; j++) {
                    if (distances[j] > distances[i]) {
                        rank++;
                    }
                }
                baseRanks[i] = rank;
            }

            // Third pass: detect ties and build rank string with "ex" if needed
            for (int i = 0; i < n; i++) {
                int sameCount = 0;
                for (int j = 0; j < n; j++) {
                    if (i != j && distances[j] == distances[i]) {
                        sameCount++;
                    }
                }

                String rang;
                if (sameCount > 0) {
                    rang = baseRanks[i] + "ex";
                } else {
                    rang = Integer.toString(baseRanks[i]);
                }

                // Emit tuple: (id, top, rang, total, maxcel)
                collector.emit(new Values(ids[i], tops[i], rang, totals[i], maxcels[i]));

                logger.info("GiveRankBolt emitted: id=" + ids[i]
                        + ", top=" + tops[i]
                        + ", rang=" + rang
                        + ", total=" + totals[i]
                        + ", maxcel=" + maxcels[i]);
            }

            collector.ack(t);

        } catch (Exception e) {
            System.err.println("Erreur dans GiveRankBolt : " + e.getMessage());
            e.printStackTrace();

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer arg1) {
		arg1.declare(new Fields("id", "top", "rang", "total", "maxcel"));
	}



    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {

    }
    

}