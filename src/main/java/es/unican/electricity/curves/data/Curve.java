package es.unican.electricity.curves.data;

import es.unican.electricity.curves.utils.Converter;
import es.unican.electricity.curves.utils.ListUtils;

import java.io.Serializable;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;

/**
 * Created by algorrim on 8/03/18.
 */
public class Curve implements Serializable{

    private String cups;
    private Date dia;
    private Double[] values;
    private Integer[] flags;
    private Integer[] prls;
    private Date created;
    private Long before_select_consumption;
    private Long after_select_consumption;
    private Long after_select_profile;

    public Curve(){}

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
            this.created = new Date(Long.parseLong(lines[77]));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Date getPreviousYear(){
        Calendar cal = Calendar.getInstance();
        cal.setTime(dia);
        cal.add(Calendar.YEAR, -1);
        return new Date(cal.getTime().getTime());
    }

    public String getCups() {
        return cups;
    }

    public void setCups(String cups) {
        this.cups = cups;
    }

    public Date getDia() {
        return dia;
    }

    public void setDia(Date dia) {
        this.dia = dia;
    }

    public Double[] getValues() {
        return values;
    }

    public void setValues(Double[] values) {
        this.values = values;
    }

    public Integer[] getFlags() {
        return flags;
    }

    public void setFlags(Integer[] flags) {
        this.flags = flags;
    }

    public Integer[] getPrls() {
        return prls;
    }

    public void setPrls(Integer[] prls) {
        this.prls = prls;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Long getBefore_select_consumption() {
        return before_select_consumption;
    }

    public void setBefore_select_consumption(Long before_select_consumption) {
        this.before_select_consumption = before_select_consumption;
    }

    public Long getAfter_select_consumption() {
        return after_select_consumption;
    }

    public void setAfter_select_consumption(Long after_select_consumption) {
        this.after_select_consumption = after_select_consumption;
    }

    public Long getAfter_select_profile() {
        return after_select_profile;
    }

    public void setAfter_select_profile(Long after_select_profile) {
        this.after_select_profile = after_select_profile;
    }

    @Override
    public String toString(){
        return "Cups: "+cups+"\tDia: "+dia+"\tValues: "+ ListUtils.mkString(Arrays.asList(values), val -> val.toString(),",")+
                "\tFlags: "+ListUtils.mkString(Arrays.asList(flags), val -> val.toString(),",")+
                "\tPrelacion: "+ListUtils.mkString(Arrays.asList(prls), val -> val.toString(),",");
    }
}
