package es.unican.electricity.curves;

import es.unican.electricity.curves.utils.StringLineParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by algorrim on 8/03/18.
 */
public class CurvesTopology {
    private static final String KAFKA_SPOUT = "KAFKA_SPOUT";
    private static final String PARSER_BOLT = "LINE_PARSER_BOLT";
    private static final int[] regions = {0,1,2,3,4,5};

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        //Spout that reads data from Kafka
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(KafkaSpoutConfig.builder("127.0.0.1:" + "9092",
                "electricity-curves")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "stormPollution").build()), 1);
        //Bolt that parses the data received from Kafka
        builder.setBolt(PARSER_BOLT, new StringLineParser()).shuffleGrouping(KAFKA_SPOUT);


        StormSubmitter.submitTopology("pollution", new Config(),builder.createTopology());
        Thread.sleep(60000);
        System.exit(0);
    }
}
