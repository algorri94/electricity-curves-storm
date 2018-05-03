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
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
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
        Connection conn = JDBCPool.connect();
        try {
            Curve curve = (Curve) tuple.getValueByField("curve");
            curve.setBefore_select_consumption(System.currentTimeMillis());
            Consumption consumption = getConsumption(conn, curve);
            curve.setAfter_select_consumption(System.currentTimeMillis());
            Profile profile = getProfile(conn, curve);
            curve.setAfter_select_profile(System.currentTimeMillis());
            if(consumption != null) {
                collector.emit(tuple(curve, consumption, profile));
            }
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
        Date previousYear = record.getPreviousYear();
        ResultSet rs = conn.createStatement()
                .executeQuery("SELECT * FROM consumos WHERE cups='"+record.getCups()+"'");
        while(rs.next() && c == null){
            if(rs.getDate(2).compareTo(previousYear) <= 0 && rs.getDate(3).compareTo(previousYear) >= 0){
                c = new Consumption(rs);
            }
        }
        return c;
    }

    private Profile getProfile(Connection conn, Curve record) throws SQLException {
        Profile p = null;
        if(perfiles.get(record.getDia().toString())!=null) {
            p = perfiles.get(record.getDia().toString());
        } else {
            ResultSet rs = conn.createStatement()
                    .executeQuery("SELECT * FROM perfiles WHERE fecha='" + record.getDia() + "'");
            if (rs.next()) p = new Profile(rs);
            perfiles.put(record.getDia().toString(),p);
        }
        return p;
    }
}
