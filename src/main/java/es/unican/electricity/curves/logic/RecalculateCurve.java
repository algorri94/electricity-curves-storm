package es.unican.electricity.curves.logic;

import es.unican.electricity.curves.data.Consumption;
import es.unican.electricity.curves.data.Curve;
import es.unican.electricity.curves.data.Profile;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

/**
 * Created by algorrim on 14/03/18.
 */
public class RecalculateCurve implements IBasicBolt{

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Curve curve = (Curve) tuple.getValueByField("curve");
        Consumption consumption = (Consumption) tuple.getValueByField("consumption");
        Profile profile = (Profile) tuple.getValueByField("profile");
        curve = recalculateCurve(curve, consumption, profile);
        collector.emit(tuple(curve));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("curve"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private Curve recalculateCurve(Curve curve, Consumption consumption, Profile profile) {
        Double[] calculated = Arrays.stream(profile.getValues()).map(value -> value*consumption.getValue()).toArray(Double[]::new);
        Double[] values = curve.getValues();
        Integer[] prls = curve.getPrls();
        for(int i = 0; i < curve.getValues().length; i++){
            if(curve.getFlags()[i]!=0){
                values[i] = calculated[i];
                prls[i] = 11;
            }
        }
        curve.setValues(values);
        curve.setPrls(prls);
        return curve;
    }
}
