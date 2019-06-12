package samzaapp;

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

public class WordCount implements StreamApplication {
    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final String INPUT_STREAM_ID = "WordCount";
    private static final String INPUT_STREAM_ID_2 = "map-output";
    private static final String OUTPUT_STREAM_ID = "word-count-output";
    private static final String OUTPUT_STREAM_ID2 = "map-output";

    @Override
    public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
        Serde serde = KVSerde.of(new StringSerde(), new StringSerde());

        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
            .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaInputDescriptor<KV<String, String>> inputDescriptor =
                kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID,
                        serde);

        KafkaInputDescriptor<KV<String, String>> inputDescriptor2 =
              kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID_2,
                      serde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID,
                        serde);

        KafkaOutputDescriptor<KV<String, String>> outputDescriptor2 =
                kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID2,
                        serde);

        MessageStream<KV<String, String>> lines = streamApplicationDescriptor.getInputStream(inputDescriptor);
        OutputStream<KV<String, String>> counts = streamApplicationDescriptor.getOutputStream(outputDescriptor);
        MessageStream<KV<String, String>> lines2 = streamApplicationDescriptor.getInputStream(inputDescriptor2);
        OutputStream<KV<String, String>> counts2 = streamApplicationDescriptor.getOutputStream(outputDescriptor2);

//    JoinFunction<? extends K, ? super M, ? super OM, ? extends JM> joinFn
//        JoinFunction<String, KV<String, String>, KV<String, String>, KV<String, String>> joinFunc =
//                new JoinFunction<String, KV<String, String>, KV<String, String>, KV<String, String>>() {
//                    @Override
//                    public KV<String, String> apply(KV<String, String> lines, KV<String, String> lines2) {
//                        return KV.of(lines.getKey()+lines2.getKey(), lines.getValue()+lines.getValue());
//                    }
//
//                    @Override
//                    public String getFirstKey(KV<String, String> lines) {
//                        return lines.getKey();
//                    }
//
//                    @Override
//                    public String getSecondKey(KV<String, String> lines2) {
//                        return lines2.getKey();
//                    }
//                };

//        lines
////                .partitionBy(m -> {System.out.println("bbbbb"); return m.key;}, m -> m.value, KVSerde.of(new StringSerde(), new StringSerde()), "p1")
//                .map(kv -> {
//                    System.out.println("hahahahah");
//                    return kv.value;
//                })
//                .flatMap(s -> {return Arrays.asList(s.split("\\W+"));})
//                .window(Windows.keyedSessionWindow(
//                        w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
//                        new StringSerde(), new IntegerSerde()), "count")
//                .map(windowPane ->
//                        KV.of(windowPane.getKey().getKey(),
//                                windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
////        .join(lines2, joinFunc, new StringSerde(), KVSerde.of(new StringSerde(), new StringSerde()), KVSerde.of(new StringSerde(), new StringSerde()), Duration.ofSeconds(5), "join")
//                .sendTo(counts);
        lines
                .map(kv -> {
                    System.out.println("stage1");
                    return kv;
                })
//                .map(kv -> {
//                    System.out.println("stage2");
//                    return kv;
//                })
                .sendTo(counts2);

        lines2
                .map(kv -> {
                    System.out.println("stage2");
                    return kv;
                })
//                .flatMap(s -> Arrays.asList(s.split("\\W+")))
//                .window(Windows.keyedSessionWindow(
//                        w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
//                        new StringSerde(), new IntegerSerde()), "count")
//                .map(windowPane ->
//                        KV.of(windowPane.getKey().getKey(),
//                                windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
                .sendTo(counts);

//        lines
//                .map(kv -> {
//                    return KV.of(kv.getKey(), kv.getValue().split("\\|")[0]);
//                })
//                .map(kv -> {
//                    return kv.value;
//                })
//                .flatMap(s -> Arrays.asList(s.split("\\W+")))
//                .window(Windows.keyedSessionWindow(
//                        w -> w, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
//                        new StringSerde(), new IntegerSerde()), "count")
//                .map(windowPane ->
//                        KV.of(windowPane.getKey().getKey(),
//                                windowPane.getKey().getKey() + ": " + windowPane.getMessage().toString()))
//                .sendTo(counts);
    }

//    public static void main(String[] args) {
//        CommandLine cmdLine = new CommandLine();
//        OptionSet options = cmdLine.parser().parse(args);
//        Config config = cmdLine.loadConfig(options);
//        Map<String, String> mergedConfig = new HashMap<>(config);
//        mergedConfig.put("splitPart", String.valueOf(0));
//        mergedConfig.put("job.name", "word-count"+0);
//        Config newConfig = Util.rewriteConfig(new MapConfig(mergedConfig));
//        LocalApplicationRunner runner = new LocalApplicationRunner(new WordCount(), newConfig);
//        runner.run();
//        runner.waitForFinish();
//    }
}
