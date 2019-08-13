// analysing outlier of machines in Google cluster using the Google Cluster Trace dataset

package samzaapp.googletrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;
import org.apache.samza.util.Util;

public class MachineOutlier implements StreamApplication {
    private static final double dupper = Math.sqrt(2);
    private double minDataInstanceScore = Double.MAX_VALUE;
    private double maxDataInstanceScore = 0;

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String INPUT_STREAM_ID = "stock_order";
    private static final String OUTPUT_STREAM_ID = "stock_price";


    @Override
    public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());
        JsonSerdeV2<Machine> MachineSerde = new JsonSerdeV2<>(Machine.class);

        DataInstanceScorer dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer("machineMetadata");

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<Machine> inputDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
                        MachineSerde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);

        MessageStream<PageView> inputStream = streamApplicationDescriptor.getInputStream(inputDescriptor);
        OutputStream<KV<String, String>> outputStream = streamApplicationDescriptor.getOutputStream(outputDescriptor);

        inputStream
                .map(machine->{
                    return new MachineMetadata(machine.timestamp, machine.id, machine.cpu, machine.memory);
                })
                .map(machineMetadata->{
                    observationList = new ArrayList<>();
                    observationList.add(machineMetadata);
                    List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                    for (ScorePackage scorePackage : scorePackageList) {
                        return scorePackage;
                    }
                })
//                .window(Windows.keyedSessionWindow(
//                        w -> w, Duration.ofSeconds(5), () -> 0, (m.getId(), prevCount) -> prevCount + m.getScore(),
//                        new StringSerde(), new IntegerSerde()), "count")
                .map(scorePackage->{
//                    List<score>  windowPane.getMessage();
                    double minScore = scorePackage.getScore();
                    double medianScore = scorePackage.getScore();
                    double streamScore = scorePackage.getScore();
                    double curDataInstScore = scorePackage.getScore();
                    boolean isAbnormal = false;

                    // current stream score deviates from the majority
                    if ((streamScore > 2 * medianScore - minScore) && (streamScore > minScore + 2 * dupper)) {
                        // check whether cur data instance score return to normal
                        if (curDataInstScore > 0.1 + minDataInstanceScore) {
                            isAbnormal = true;
                        }
                    }
                    return KV.of(" ", "abnormal machines.");
                })
                .sendTo(outputStream);
    }

    private KV<String, String> computeAverage(KV m) {
        String[] orderArr = (String[]) m.getValue();
        if (!stockAvgPriceMap.containsKey(orderArr[Sec_Code])) {
            stockAvgPriceMap.put(orderArr[Sec_Code], (float) 0);
        }
        float sum = stockAvgPriceMap.get(orderArr[Sec_Code]) + Float.parseFloat(orderArr[Order_Price]);
        stockAvgPriceMap.put(orderArr[Sec_Code], sum);
        return new KV(m.getKey(), String.valueOf(sum));
    }

    /**
     * Identify the abnormal streams.
     * @return
     */
//    private List<Tuple> identifyAbnormalStreams() {
//        List<Tuple> abnormalStreamList = new ArrayList<>();
//        int medianIdx = (int)(streamList.size() / 2);
//        BFPRT.bfprt(streamList, medianIdx);
//        abnormalStreamList.addAll(streamList);
//        return abnormalStreamList;
//    }
}
