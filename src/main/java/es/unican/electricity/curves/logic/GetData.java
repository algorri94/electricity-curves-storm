package es.unican.electricity.curves.logic;

import es.unican.electricity.curves.data.Consumption;
import es.unican.electricity.curves.data.Curve;
import es.unican.electricity.curves.data.Profile;
import es.unican.electricity.curves.utils.JDBCPool;
import org.apache.commons.dbcp2.Utils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
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
        try {
            Curve curve = (Curve) tuple.getValueByField("curve");
            Consumption consumption = null;
            consumption = getConsumption(conn, curve);
            Profile profile = getProfile(conn, curve);
            if(consumption != null) collector.emit(tuple(curve, consumption, profile));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Utils.closeQuietly(conn);
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

    private Consumption getConsumption(Connection conn, Curve record) throws SQLException {
        Consumption c = null;
        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT * FROM consumos WHERE cups='"+record.getCups()+"' AND f_desde<='"+
                        record.getStringPreviousYear()+"' AND f_hasta>='"+record.getStringPreviousYear()+"' LIMIT 1");
        if (rs.next()) c = new Consumption(rs);
        return c;
    }

    private Profile getProfile(Connection conn, Curve record) throws SQLException {
        Profile p = null;
        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT * FROM perfiles WHERE fecha='"+record.getDia()+"' LIMIT 1");
        if(rs.next()) p = new Profile(rs);
        return p;
    }
}
