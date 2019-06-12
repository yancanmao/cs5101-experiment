package generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * SSE generaor
 */
class SSEGnerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;
    private static final int Last_Upd_Time = 3;
    public SSEGnerator(String input, String bootstrapServer) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "generator.SSEPartitioner");
        producer = new KafkaProducer<String, String>(props);

    }

    public void generate(String file, int speed, long startPoint) throws InterruptedException {

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        long interval = 0;
        int counter = 0;
        try {
            stream = new FileReader(file);
            br = new BufferedReader(stream);
//            Thread.sleep(10000);
            System.out.println("Start point: " + startPoint);
            boolean isStart = false;
            while ((sCurrentLine = br.readLine()) != null) {
                long time = 0;
                if(sCurrentLine.split("\\|").length >= 10) {
                    String t = sCurrentLine.split("\\|")[Last_Upd_Time];
                    time = Duration.between(LocalTime.MIN, LocalTime.parse(t)).toMillis() / 1000;
                }
                if(time > startPoint)isStart = true;
                if(isStart) {
                    if (sCurrentLine.equals("end")) {
                        counter++;
                    }
                    if (counter == speed) {
                        start = System.nanoTime();
                        interval = 1000000000 / textList.size();
                        for (int i = 0; i < textList.size(); i++) {
                            cur = System.nanoTime();
                            if (textList.get(i).split("\\|").length < 10) {
                                continue;
                            }
                            StringBuilder key = new StringBuilder(textList.get(i).split("\\|")[Sec_Code]);
                            //key.append("|");
                            //key.append(cur);
                            ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, key.toString(), textList.get(i));
                            producer.send(newRecord);
                            producer.flush();
                            while ((System.nanoTime() - cur) < interval) {
                            }
                        }
                        //}
                        System.out.println("size:" + String.valueOf(textList.size()));
                        System.out.println("time:" + String.valueOf((System.nanoTime() - start) / 1000000));
                        System.out.println("interval:" + String.valueOf((System.nanoTime() - start) / (textList.size())));
                        textList.clear();
                        counter = 0;
                        continue;
                    }
                    textList.add(sCurrentLine);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }
        producer.close();
        //logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    public void warmup(String file, int speed, long period) throws IOException {
        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        long interval = 0;
        int counter = 0;
        long begin = System.currentTimeMillis();
        try {
            System.out.println("warm up start...");
            stream = new FileReader(file);
            br = new BufferedReader(stream);
            interval = 1000000000 / speed;
            start = System.nanoTime();

            while ((sCurrentLine = br.readLine()) != null) {
                if (System.currentTimeMillis() - begin < period) {

                    cur = System.nanoTime();
                    if (sCurrentLine.equals("end")) {
                        continue;
                    }

                    if (sCurrentLine.split("\\|").length < 10) {
                        continue;
                    }

                    ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, sCurrentLine.split("\\|")[Sec_Code], sCurrentLine);
                    producer.send(newRecord);
                    counter++;

                    while ((System.nanoTime() - cur) < interval) {
                    }
                    if (System.nanoTime() - start >= 1000000000) {
                        System.out.println("output rate: " + counter);
                        counter = 0;
                        start = System.nanoTime();
                    }
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        String TOPIC = new String("stock");
        String file = new String("partition1");
        int speed = 1;
        long startPoint = 0;  //In second
        long warmupPeriod = 10000;
        int warmupSpeed = 20;
        String bootstrapServer = "localhost:9092";
        if (args.length > 0) {
            TOPIC = args[0];
            file = args[1];
            speed = Integer.parseInt(args[2]);
            startPoint = Long.parseLong(args[3]);
            warmupSpeed = Integer.parseInt(args[4]);
            warmupPeriod = Long.parseLong(args[5]);
            bootstrapServer = args[6];
        }
        SSEGnerator generator = new SSEGnerator(TOPIC, bootstrapServer);
        generator.warmup(file, warmupSpeed, warmupPeriod); //period(ms)
        generator.generate(file, speed, startPoint);
    }
}
