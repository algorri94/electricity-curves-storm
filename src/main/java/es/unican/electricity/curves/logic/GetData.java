package es.unican.electricity.curves.logic;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import es.unican.electricity.curves.data.Consumption;
import es.unican.electricity.curves.data.Curve;
import es.unican.electricity.curves.data.Profile;
import es.unican.electricity.curves.utils.JDBCPool;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

/**
 * Created by algorrim on 13/03/18.
 */
public class GetData implements IBasicBolt{

    Map<String, Profile> perfiles;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        perfiles = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Session conn = JDBCPool.connect();
        Curve curve = (Curve) tuple.getValueByField("curve");
        curve.setBefore_select_consumption(System.currentTimeMillis());
        Consumption consumption = getConsumption(conn, curve);
        curve.setAfter_select_consumption(System.currentTimeMillis());
        Profile profile = getProfile(conn, curve);
        curve.setAfter_select_profile(System.currentTimeMillis());
        if(consumption != null) {
            collector.emit(tuple(curve, consumption, profile));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("curve", "consumption", "profile"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private Consumption getConsumption(Session conn, Curve record) {
        Consumption c = null;
        Date previousYear = record.getPreviousYear();
        ResultSet rs = conn.execute("SELECT * FROM consumos WHERE cups='"+record.getCups()+"'");
        for(Row row : rs){
            if(new Date(row.getDate(1).getMillisSinceEpoch()).compareTo(previousYear) <= 0 &&
                    new Date(row.getDate(2).getMillisSinceEpoch()).compareTo(previousYear) >= 0){
                c = new Consumption(row);
            }
        }
        return c;
    }

    private Profile getProfile(Session conn, Curve record) {
        Profile p = null;
        if(perfiles.get(record.getDia().toString())!=null) {
            p = perfiles.get(record.getDia().toString());
        } else {
            ResultSet rs = conn.execute("SELECT * FROM perfiles WHERE fecha='" + record.getDia() + "'");
            Row row = rs.one();
            if (row!=null) p = new Profile(row);
            perfiles.put(record.getDia().toString(),p);
        }
        return p;
    }
}
