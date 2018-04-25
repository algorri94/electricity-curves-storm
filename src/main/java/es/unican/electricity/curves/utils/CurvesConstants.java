package es.unican.electricity.curves.utils;

/**
 * Created by algorrim on 12/03/18.
 */
public class CurvesConstants {

    public static int DB_PORT = 3306;
    public static String DB_ADDRESS = "c2-43";
    public static String DB_NAME = "electricity";
    public static String JDBC_ADDRESS = "jdbc:mysql://"+DB_ADDRESS+":"+DB_PORT+"/"+DB_NAME;
    public static String DB_USERNAME = "root";
    public static String DB_PASSWD = "root";
    public static String KAFKA_SERVERS = "192.168.0.48:9092,192.168.0.49:9092,192.168.0.50:9092,192.168.0.51:9092,192.168.0.52:9092";
}
