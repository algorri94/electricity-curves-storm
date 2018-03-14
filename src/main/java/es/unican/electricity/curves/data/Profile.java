package es.unican.electricity.curves.data;

import java.io.Serializable;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by algorrim on 13/03/18.
 */
public class Profile implements Serializable{

    private Date date;
    private Double[] values;

    public Profile(Date date, Double[] values) {
        this.date = date;
        this.values = values;
    }

    public Profile(ResultSet rs) throws SQLException {
        this(rs.getDate(1), new Double[]{rs.getDouble(2), rs.getDouble(3), rs.getDouble(4), rs.getDouble(5), rs.getDouble(6), rs.getDouble(7),
                rs.getDouble(8), rs.getDouble(9), rs.getDouble(10), rs.getDouble(11), rs.getDouble(12), rs.getDouble(13), rs.getDouble(14),
                rs.getDouble(15), rs.getDouble(16), rs.getDouble(17), rs.getDouble(18), rs.getDouble(19), rs.getDouble(20), rs.getDouble(21),
                rs.getDouble(22), rs.getDouble(23), rs.getDouble(24), rs.getDouble(25), rs.getDouble(26)});
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Double[] getValues() {
        return values;
    }

    public void setValues(Double[] values) {
        this.values = values;
    }
}
