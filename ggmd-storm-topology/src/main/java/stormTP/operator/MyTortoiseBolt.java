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
public class MyTortoiseBolt implements IRichBolt {

    private static final long serialVersionUID = 4262369370788107343L;

	private static Logger logger = Logger.getLogger("MyTortoiseBoltLogger");
	//Attributs de MyTortoiseBolt
    private OutputCollector collector;
    private int tortoiseID; 
    private String nomTortue;

    //Constructeur de MyTortoiseboLT
    public MyTortoiseBolt (int tortoiseID, String nomTortue){
        this.tortoiseID = tortoiseID;
        this.nomTortue = nomTortue;
    }

    public void prepare(Map arg1, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
    }


    public void execute(Tuple t) {
	
		try {

			String jsont = t.getValueByField("json").toString();
            JsonReader reader = Json.createReader(new StringReader(jsont));
            // Parse the JSON string into a JsonObject
            JsonObject jsonObject = reader.readObject();
            JsonArray runners = jsonObject.getJsonArray("runners");
            for (int i = 0; i < runners.size(); i++) {
                JsonObject runner = runners.getJsonObject(i);
                int idCourant = runner.getInt("id");
                if (idCourant == tortoiseID) {
                    int tour = runner.getInt("tour");
                    int top = runner.getInt("top");
                    int cellule = runner.getInt("cellule");
                    int maxcel = runner.getInt("maxcel");
                    int total  = runner.getInt("total");
                    int nbCellsParcourus = (tour * maxcel) + cellule;
                    collector.emit(new Values(idCourant, top, nomTortue, nbCellsParcourus, total, maxcel));
                    logger.info("Tortue " + nomTortue + " traitÃ©e : id=" + idCourant);

                    break;
                }
            }
			collector.ack(t);
		}catch (Exception e){
			System.err.println("Erreur dans MyTortoiseBolt : " + e.getMessage());
		}
		return;
		
	}

    public void declareOutputFields(OutputFieldsDeclarer arg1) {
		arg1.declare(new Fields("id", "top", "nom", "nbCellsParcourus", "total", "maxcel"));
	}

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {

    }
    

}