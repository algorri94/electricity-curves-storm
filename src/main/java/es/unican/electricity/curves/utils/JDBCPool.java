package es.unican.electricity.curves.utils;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by algorrim on 13/03/18.
 */
public class JDBCPool {

    private static final int DEFAULT_JDBC_LOGIN_TIMEOUT = 10; //seconds
    private static BasicDataSource pool = null;

    private JDBCPool(){}

    private static BasicDataSource createPool(){
        DriverManager.setLoginTimeout(DEFAULT_JDBC_LOGIN_TIMEOUT);

        BasicDataSource newPool = new BasicDataSource();
        newPool.setDriverClassName("com.mysql.jdbc.Driver");
        newPool.setUrl(CurvesConstants.JDBC_ADDRESS);
        newPool.setUsername(CurvesConstants.DB_USERNAME);
        newPool.setPassword(CurvesConstants.DB_PASSWD);
        newPool.addConnectionProperty("zeroDateTimeBehavior", "convertToNull");
        newPool.setMaxTotal(-1);
        newPool.setMaxConnLifetimeMillis(1000 * 60 * 60);

        return newPool;
    }

    public static Connection connect(){
        Connection conn = null;
        try {
            if (pool == null) {
                pool = createPool();
            }
            conn = pool.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
