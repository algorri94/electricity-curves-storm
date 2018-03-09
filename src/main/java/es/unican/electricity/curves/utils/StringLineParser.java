package es.unican.electricity.curves.utils;

import static org.apache.storm.utils.Utils.tuple;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class StringLineParser implements IBasicBolt {

    private static String splitChar = ",";

    public void prepare(Map stormConf, TopologyContext context) {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = (String) input.getValue(4);
        String[] fields = line.split(splitChar);
        collector.emit(tuple(new Curve(fields)));
    }

    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("curve"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}