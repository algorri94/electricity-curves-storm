package es.unican.electricity.curves.logic;

import es.unican.electricity.curves.data.Curve;
import es.unican.electricity.curves.utils.JDBCPool;
import es.unican.electricity.curves.utils.ListUtils;
import org.apache.commons.dbcp2.Utils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
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
            conn.createStatement().execute("INSERT INTO curvas (CUPS,DIA,VALOR_H01,VALOR_H02,VALOR_H03,VALOR_H04,VALOR_H05," +
                    "VALOR_H06,VALOR_H07,VALOR_H08,VALOR_H09,VALOR_H10,VALOR_H11,VALOR_H12,VALOR_H13,VALOR_H14,VALOR_H15,VALOR_H16,VALOR_H17," +
                    "VALOR_H18,VALOR_H19,VALOR_H20,VALOR_H21,VALOR_H22,VALOR_H23,VALOR_H24,VALOR_H25,FLAG_H01,FLAG_H02,FLAG_H03,FLAG_H04," +
                    "FLAG_H05,FLAG_H06,FLAG_H07,FLAG_H08,FLAG_H09,FLAG_H10,FLAG_H11,FLAG_H12,FLAG_H13,FLAG_H14,FLAG_H15,FLAG_H16,FLAG_H17," +
                    "FLAG_H18,FLAG_H19,FLAG_H20,FLAG_H21,FLAG_H22,FLAG_H23,FLAG_H24,FLAG_H25,PRL_H01,PRL_H02,PRL_H03,PRL_H04,PRL_H05,PRL_H06," +
                    "PRL_H07,PRL_H08,PRL_H09,PRL_H10,PRL_H11,PRL_H12,PRL_H13,PRL_H14,PRL_H15,PRL_H16,PRL_H17,PRL_H18,PRL_H19,PRL_H20,PRL_H21," +
                    "PRL_H22,PRL_H23,PRL_H24,PRL_H25,created, before_select_consumption, after_select_consumption, after_select_profile) VALUES("+
                    "'"+curve.getCups()+"',"+
                    "'"+curve.getDia()+"',"+
                    ListUtils.mkString(Arrays.asList(curve.getValues()), val -> val.toString(),",")+","+
                    ListUtils.mkString(Arrays.asList(curve.getFlags()), val -> val.toString(),",")+","+
                    ListUtils.mkString(Arrays.asList(curve.getPrls()), val -> val.toString(),",")+","+
                    "'"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(curve.getCreated())+"000'" +","+
                    "'"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(curve.getBefore_select_consumption()))+"000'" +","+
                    "'"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(curve.getAfter_select_consumption()))+"000'" +","+
                    "'"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(curve.getAfter_select_profile()))+"000')");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Utils.closeQuietly(conn);
        }
    }
}
