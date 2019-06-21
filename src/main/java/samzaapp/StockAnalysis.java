package samzaapp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.*;
import java.time.Duration;
import java.util.*;

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
import java.util.Random;

public class StockAnalysis implements StreamApplication {
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    Map<String, Float> stockAvgPriceMap = new HashMap<String, Float>();
    Map<String, Long> stockVolumeMap = new HashMap<String, Long>();

    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    private static final int MINIMAL_PRICE = 10;

    private static final String INPUT_STREAM_ID = "stock_order";
        private static final String INPUT_STREAM_ID_2 = "im_stream";
    private static final String OUTPUT_STREAM_ID2 = "im_stream";
    private static final String OUTPUT_STREAM_ID = "stock";


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

        MessageStream<KV<String, String>> inputStream = streamApplicationDescriptor.getInputStream(inputDescriptor);
        MessageStream<KV<String, String>> imStreamIn = streamApplicationDescriptor.getInputStream(inputDescriptor2);
        OutputStream<KV<String, String>> imStreamOut = streamApplicationDescriptor.getOutputStream(outputDescriptor2);
        OutputStream<KV<String, String>> outputStream = streamApplicationDescriptor.getOutputStream(outputDescriptor);

        // "transactor"
        inputStream
            .map(order -> {
                Random ra =new Random();
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < ra.nextInt(5) + 10){}
                return order;
            }).sendTo(imStreamOut);

        //  movingAverage
        imStreamIn
            .map(order -> {
                String[] orderArr = order.getValue().split("\\|");
                return new KV(order.getKey(), orderArr);
            })
            .map(this::movingAverage)
            .sendTo(outputStream);

        // composite index
        imStreamIn
            .map(order -> {
                String[] orderArr = order.getValue().split("\\|");
                return new KV(order.getKey(), orderArr);
            })
            .map(this::compositeIndex)
            .sendTo(outputStream);

        // price alarm
        imStreamIn
            .map(order -> {
                String[] orderArr = order.getValue().split("\\|");
                return new KV(order.getKey(), orderArr);
            })
            .map(this::priceAlarm)
            .sendTo(outputStream);

        // fraud detection
        imStreamIn
            .map(order -> {
                String[] orderArr = order.getValue().split("\\|");
                return new KV(order.getKey(), orderArr);
            })
            .map(this::fraudDetection)
            .sendTo(outputStream);
    }

    private KV<String, String> movingAverage(KV m) {
        String[] orderArr = (String[]) m.getValue();
        if (!stockAvgPriceMap.containsKey(orderArr[Sec_Code])) {
            stockAvgPriceMap.put(orderArr[Sec_Code], (float) 0);
        }
        float sum = stockAvgPriceMap.get(orderArr[Sec_Code]) + Float.parseFloat(orderArr[Order_Price]);
        stockAvgPriceMap.put(orderArr[Sec_Code], sum);
        return new KV(m.getKey(), String.valueOf(sum));
    }

    private KV<String, String> compositeIndex(KV m) {
        String[] orderArr = (String[]) m.getValue();
        // user transaction statistics
        Long sum = stockVolumeMap.get(orderArr[Sec_Code]) + Long.parseLong(orderArr[Order_Vol]);
        return new KV(m.getKey(), String.valueOf(sum));
    }

    private KV<String, String> priceAlarm(KV m) {
        String[] orderArr = (String[]) m.getValue();
        // user transaction statistics
        if (Float.parseFloat(orderArr[Order_Price]) < MINIMAL_PRICE) {
            return new KV(m.getKey(), "low stock exchange price alarm!");
        } else {
            return new KV(m.getKey(), "normal transaction");
        }
    }

    private KV<String, String> fraudDetection(KV m) {
        String[] orderArr = (String[]) m.getValue();
        // design a kmeans algorithm here
        if (Float.parseFloat(orderArr[Order_Price]) < MINIMAL_PRICE) {
            return new KV(m.getKey(), "low stock exchange price alarm!");
        } else {
            return new KV(m.getKey(), "normal transaction");
        }
    }
}
