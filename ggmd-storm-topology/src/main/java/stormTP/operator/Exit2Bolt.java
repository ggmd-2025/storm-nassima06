package stormTP.operator;


import java.util.Map;
//import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import stormTP.stream.StreamEmiter;


public class Exit2Bolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107342L;
	//private static Logger logger = Logger.getLogger("ExitBolt");
	private OutputCollector collector;
	int port = -1;
	StreamEmiter semit = null;
    public Exit2Bolt (int port) {
		this.port = port;
		this.semit = new StreamEmiter(this.port);
		
	}

    public void execute(Tuple t) {
	
        try {
            int id = t.getIntegerByField("id");
            int top = t.getIntegerByField("top");
            String nom = t.getStringByField("nom");
            int nbCellsParcourus = t.getIntegerByField("nbCellsParcourus");
            int total = t.getIntegerByField("total");
            int maxcel = t.getIntegerByField("maxcel");
            // Construction du JSON
            JsonObjectBuilder jsonC = Json.createObjectBuilder();
            jsonC.add("id", id);
            jsonC.add("top", top);
            jsonC.add("nom", nom);
            jsonC.add("nbCellsParcourus", nbCellsParcourus);
            jsonC.add("total", total);
            jsonC.add("maxcel", maxcel);
            JsonObject objetFinal = jsonC.build();
            String jsonFinal = objetFinal.toString();
            //envoyer sur le port 
            this.semit.send(jsonFinal);
            collector.emit(new Values(jsonFinal));
            collector.ack(t);
		} catch (Exception e) {
            e.printStackTrace();
        }
		return;
		
	}

    /* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg1) {
		arg1.declare(new Fields("json"));
	}
		

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {
		
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg1, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

}