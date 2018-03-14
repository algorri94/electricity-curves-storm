package es.unican.electricity.curves.logic;

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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

/**
 * Created by algorrim on 13/03/18.
 */
public class GetData implements IBasicBolt{

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Connection conn = JDBCPool.connect();
        Curve curve = (Curve) tuple.getValueByField("curve");
        Consumption consumption = getConsumption(conn, curve);
        Profile profile = getProfile(conn, curve);
        collector.emit(tuple(curve, consumption, profile));
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

    private Consumption getConsumption(Connection conn, Curve record) {
        Consumption c = null;
        try {
            c = new Consumption(conn.createStatement()
                    .executeQuery("SELECT * FROM consumos WHERE cups='"+record.getCups()+"' AND f_desde<='"+
                            record.getStringPreviousYear()+"' AND f_hasta>='"+record.getStringPreviousYear()+"' LIMIT 1"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return c;
    }

    private Profile getProfile(Connection conn, Curve record) {
        Profile p = null;
        try {
            ResultSet resultProf = conn.createStatement()
                    .executeQuery("SELECT * FROM perfiles WHERE fecha='"+record.getDia()+"' LIMIT 1");
            p = new Profile(resultProf);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return p;
    }
}
