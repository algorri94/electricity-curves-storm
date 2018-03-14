package es.unican.electricity.curves.data;

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
    private Integer value;
    private Integer prl;

    public Consumption(String cups, Date d_from, Date d_to, Integer value, Integer prl){
        this.cups = cups;
        this.d_from = d_from;
        this.d_to = d_to;
        this.value = value;
        this.prl = prl;
    }

    public Consumption (ResultSet resultSet) throws SQLException {
        this(resultSet.getString(1), resultSet.getDate(2), resultSet.getDate(3), resultSet.getInt(4), resultSet.getInt(5));
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

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public Integer getPrl() {
        return prl;
    }

    public void setPrl(Integer prl) {
        this.prl = prl;
    }
}
