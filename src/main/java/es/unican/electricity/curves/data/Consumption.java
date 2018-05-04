package es.unican.electricity.curves.data;

import com.datastax.driver.core.Row;

import java.io.Serializable;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by algorrim on 13/03/18.
 */
public class Consumption implements Serializable {

    private String cups;
    private Date d_from;
    private Date d_to;
    private Double value;
    private Integer prl;

    public Consumption(){}

    public Consumption(String cups, Date d_from, Date d_to, Double value, Integer prl){
        this.cups = cups;
        this.d_from = d_from;
        this.d_to = d_to;
        this.value = value;
        this.prl = prl;
    }

    public Consumption (ResultSet resultSet) throws SQLException {
        this(resultSet.getString(1), resultSet.getDate(2), resultSet.getDate(3), resultSet.getDouble(4), resultSet.getInt(5));
    }

    public Consumption (Row row) {
        this(row.getString("cups"), new Date(row.getDate("f_desde").getMillisSinceEpoch()), new Date(row.getDate("f_hasta").getMillisSinceEpoch()), row.getDouble("valor"), row.getInt("prelacion"));
    }

    public String getCups() {
        return cups;
    }

    public void setCups(String cups) {
        this.cups = cups;
    }

    public Date getD_from() {
        return d_from;
    }

    public void setD_from(Date d_from) {
        this.d_from = d_from;
    }

    public Date getD_to() {
        return d_to;
    }

    public void setD_to(Date d_to) {
        this.d_to = d_to;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Integer getPrl() {
        return prl;
    }

    public void setPrl(Integer prl) {
        this.prl = prl;
    }

    @Override
    public String toString(){
        return "Cups: "+cups+"\tDate From: "+d_from+"\tDate To: "+d_to+"\tValue: "+value+"\tPrl: "+prl;
    }
}
