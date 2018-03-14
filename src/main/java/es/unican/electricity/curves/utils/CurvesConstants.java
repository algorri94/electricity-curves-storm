package es.unican.electricity.curves.utils;

/**
 * Created by algorrim on 12/03/18.
 */
public class CurvesConstants {

    public static int DB_PORT = 9098;
    public static String DB_ADDRESS = "localhost";
    public static String DB_NAME = "electricity";
    public static String JDBC_ADDRESS = "jdbc:mysql://"+DB_ADDRESS+":"+DB_PORT+"/"+DB_NAME;
    public static String DB_USERNAME = "root";
    public static String DB_PASSWD = "ctr2017";
    public static String KAFKA_SERVERS = "localhost:9093";
}
