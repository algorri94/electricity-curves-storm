package es.unican.electricity.curves.utils;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

/**
 * Created by algorrim on 8/03/18.
 */
public class Curve {

    private String cups;
    private Date dia;
    private Double[] values;
    private Integer[] flags;
    private Integer[] prls;

    public Curve(String cups, Date dia, Double[] values, Integer[] flags, Integer[]prls){
        this.cups = cups;
        this.dia = dia;
        this.values = values;
        this.flags = flags;
        this.prls = prls;
    }

    public Curve(String[] lines) {
        try {
            this.cups = lines[0].replaceAll("\"", "");
            this.dia = new Date(new SimpleDateFormat("yyyyMMdd").parse(lines[1]).getTime());
            this.values = Converter.stringArrayToDoubleArray(Arrays.copyOfRange(lines, 2, 27));
            this.flags = Converter.stringArrayToIntArray(Arrays.copyOfRange(lines, 27, 52));
            this.prls = Converter.stringArrayToIntArray(Arrays.copyOfRange(lines, 52, 77));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
