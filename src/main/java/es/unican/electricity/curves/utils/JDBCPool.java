package es.unican.electricity.curves.utils;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;

/**
 * Created by algorrim on 13/03/18.
 */
public class JDBCPool {

    private static Session pool = null;

    private JDBCPool(){}

    private static Session createPool(){
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 20, 80);
        poolingOptions.setConnectionsPerHost(HostDistance.REMOTE, 20, 80);
        Cluster cluster = Cluster.builder()
                .withPoolingOptions(poolingOptions)
                .addContactPoints(CurvesConstants.DB_NODES)
                .build();

        return cluster.connect(CurvesConstants.DB_NAME);
    }

    public static Session connect(){
        if (pool == null) {
            pool = createPool();
        }
        return pool;
    }
}
