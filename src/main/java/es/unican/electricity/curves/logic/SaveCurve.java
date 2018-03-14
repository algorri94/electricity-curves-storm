package es.unican.electricity.curves.logic;

import es.unican.electricity.curves.data.Curve;
import es.unican.electricity.curves.utils.JDBCPool;
import es.unican.electricity.curves.utils.ListUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by algorrim on 14/03/18.
 */
public class SaveCurve implements IBasicBolt{
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Connection conn = JDBCPool.connect();
        Curve curve = (Curve) tuple.getValueByField("curve");
        saveCurve(conn, curve);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void saveCurve(Connection conn, Curve curve) {
        try {
            conn.createStatement().execute("INSERT INTO curvas VALUES("+
                    "'"+curve.getCups()+"',"+
                    "'"+curve.getDia()+"',"+
                    ListUtils.mkString(Arrays.asList(curve.getValues()), val -> val.toString(),",")+","+
                    ListUtils.mkString(Arrays.asList(curve.getFlags()), val -> val.toString(),",")+","+
                    ListUtils.mkString(Arrays.asList(curve.getPrls()), val -> val.toString(),",")+")");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
