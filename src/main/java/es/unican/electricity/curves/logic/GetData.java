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
import java.sql.Date;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Map;
import java.util.Random;

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
        //ResultSet rs = conn.createStatement()
        //        .executeQuery("SELECT * FROM consumos WHERE cups='"+record.getCups()+"' AND f_desde<='"+
        //                record.getStringPreviousYear()+"' AND f_hasta>='"+record.getStringPreviousYear()+"' LIMIT 1");
        //if (rs.next()) c = new Consumption(rs);
        Calendar cal = Calendar.getInstance();
        cal.setTime(record.getDia());
        cal.add(Calendar.YEAR, -1);
        cal.add(Calendar.DAY_OF_YEAR, -5);
        Date from = new Date(cal.getTime().getTime());
        cal.add(Calendar.MONTH,1);
        Date to = new Date(cal.getTime().getTime());
        Random r = new Random();
        c = new Consumption(record.getCups(),from,to,r.nextInt(2000),1);
        return c;
    }

    private Profile getProfile(Connection conn, Curve record) throws SQLException {
        Profile p = null;
        //ResultSet rs = conn.createStatement()
        //        .executeQuery("SELECT * FROM perfiles WHERE fecha='"+record.getDia()+"' LIMIT 1");
        //if(rs.next()) p = new Profile(rs);
        p = new Profile();
        p.setDate(record.getDia());
        Double[] values = {0.000111340266,0.000071835665,0.000059351453,0.000051775154,0.000044268102,0.00004421335,0.000054235248,
                0.000078857175,0.000103890024,0.000102936422,0.000099998746,0.000104543681,0.000109430982,0.000119093335,
                0.00012910094,0.00012913089,0.000119147461,0.000116666244,0.000114150077,0.000116938351,0.000134472368,
                0.000158833425,0.000169181549,0.000148896717,0.0};
        p.setValues(values);
        return p;
    }
}
