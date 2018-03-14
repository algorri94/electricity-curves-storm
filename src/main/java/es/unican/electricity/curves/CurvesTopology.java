package es.unican.electricity.curves;

import es.unican.electricity.curves.data.Profile;
import es.unican.electricity.curves.logic.GetData;
import es.unican.electricity.curves.logic.RecalculateCurve;
import es.unican.electricity.curves.logic.SaveCurve;
import es.unican.electricity.curves.logic.StringLineParser;
import es.unican.electricity.curves.data.Consumption;
import es.unican.electricity.curves.data.Curve;
import es.unican.electricity.curves.utils.CurvesConstants;
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
    private static final String GET_DATA_BOLT = "GET_DATA_BOLT";
    private static final String RECALCULATE_CURVE_BOLT = "RECALCULATE_CURVE_BOLT";
    private static final String SAVE_CURVE_BOLT = "SAVE_CURVE_BOLT";

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.registerSerialization(Curve.class);
        config.registerSerialization(Consumption.class);
        config.registerSerialization(Profile.class);

        TopologyBuilder builder = new TopologyBuilder();
        //Spout that reads data from Kafka
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(KafkaSpoutConfig.builder(CurvesConstants.KAFKA_SERVERS,
                "electricity-curves")
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "stormPollution").build()), 1);
        //Bolt that parses the data received from Kafka
        builder.setBolt(PARSER_BOLT, new StringLineParser()).shuffleGrouping(KAFKA_SPOUT);
        //Bolt that gets the data needed to recalculate the curve
        builder.setBolt(GET_DATA_BOLT, new GetData()).shuffleGrouping(PARSER_BOLT);
        //Bolt that recalculates the curve
        builder.setBolt(RECALCULATE_CURVE_BOLT, new RecalculateCurve()).shuffleGrouping(GET_DATA_BOLT);
        //Bolt that saves the curve
        builder.setBolt(SAVE_CURVE_BOLT, new SaveCurve()).shuffleGrouping(RECALCULATE_CURVE_BOLT);


        StormSubmitter.submitTopology("electricity-curves", config, builder.createTopology());
        Thread.sleep(60000);
        System.exit(0);
    }
}
