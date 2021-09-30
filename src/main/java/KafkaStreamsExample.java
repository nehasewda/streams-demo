//import io.apicurio.registry.client.CompatibleClient;
//import io.apicurio.registry.client.*;
import com.fasterxml.jackson.databind.JsonNode;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.serde.DefaultSchemaResolver;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.SerdeHeaders;
import io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializerConfig;
import io.apicurio.registry.serde.strategy.ArtifactResolverStrategy;
//import io.apicurio.registry.utils.PropertiesUtil;
//import io.apicurio.registry.utils.serde.*;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.serde.jsonschema.JsonSchemaSerde;
/*import io.apicurio.registry.utils.serde.strategy.AutoRegisterIdStrategy;
import io.apicurio.registry.utils.serde.strategy.GetOrCreateIdStrategy;
import io.apicurio.registry.utils.serde.strategy.RecordIdStrategy;
import io.apicurio.registry.utils.serde.strategy.SimpleTopicIdStrategy;*/
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.*;

public class KafkaStreamsExample {
    public static void main(String args[]) throws InterruptedException {
        String serviceKafka = System.getenv("BOOTSTRAP_SERVERS_CONFIG") ==null ? "localhost:9092" : System.getenv("BOOTSTRAP_SERVERS_CONFIG");
        String inputTopic = System.getenv("INPUT_TOPIC") ==null ? "in_stream" : System.getenv("INPUT_TOPIC");
        String schemaReg = System.getenv("REGISTRY_URL")==null ? "http://localhost:8080/" : System.getenv("REGISTRY_URL");
        String INPUT_TOPIC = inputTopic;
        KafkaStreams streams;
        Thread.sleep(30000);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serviceKafka);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerExample");
        props.put("apicurio.registry.url", schemaReg);
        props.put("apicurio.registry.headers.enabled", true);
        props.put("apicurio.registry.artifact-resolver-strategy",io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaSerializer.class);
        KafkaProducer<String, DataModel> kp = new KafkaProducer(props);
        DataModel d = new DataModel("a","Crane");



        //NOTE - Commenting kafka consumer code. Uncommenting this part will fetch the message successfully but same is not working with kafka streams

        /*Properties propConsume = new Properties();
        propConsume.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serviceKafka);
        propConsume.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerExample");
        propConsume.put("apicurio.registry.url", schemaReg);
        propConsume.put("apicurio.registry.artifact-resolver-strategy",io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy.class);
        propConsume.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propConsume.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.apicurio.registry.serde.jsonschema.JsonSchemaKafkaDeserializer.class);

        Consumer<String, DataModel> consumer = new KafkaConsumer<>(propConsume);
        consumer.subscribe(Collections.singleton(inputTopic));*/
        final ProducerRecord<String, DataModel> record = new ProducerRecord(INPUT_TOPIC, "key1", d);
        kp.send(record, (metadata, exception)->{
            if(metadata != null){
                System.out.println(metadata.toString());
            }else{
                exception.printStackTrace();
            }
        });
        /*int i=100;
        while(i > 0){
            final ConsumerRecords<String, DataModel> recordCon = consumer.poll(1000);
            i--;
            recordCon.forEach(record1 ->{
                System.out.println(record1.partition()+" "+ record1.key()+" "+record1.value()+" "+new Date().getTime());
            });
            consumer.commitAsync();
        }*/

        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serviceKafka);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "registry-demo");

        StreamsBuilder builder = new StreamsBuilder();

        Map < String, Object > serdeProps = new HashMap < > ();
        serdeProps.put(SerdeConfig.REGISTRY_URL, schemaReg);
        serdeProps.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy.class);
        serdeProps.put(SerdeConfig.VALIDATION_ENABLED, true);

        JsonSchemaSerde<DataModel> ser = new JsonSchemaSerde<>();
        ser.configure(serdeProps, false);

        Map < String, Object > serdePropsDs = new HashMap < > ();
        serdePropsDs.put(SerdeConfig.REGISTRY_URL, schemaReg);
        serdePropsDs.put(SerdeConfig.FIND_LATEST_ARTIFACT, true);
        serdePropsDs.put(SerdeConfig.VALIDATION_ENABLED, true);
        serdePropsDs.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, io.apicurio.registry.serde.strategy.SimpleTopicIdStrategy.class);

        JsonSchemaSerde<DataModel> dser = new JsonSchemaSerde<>();
        dser.configure(serdePropsDs, false);

        Serde<String> stringSerde = Serdes.String();
        KStream<String, DataModel> input = builder.stream(INPUT_TOPIC,Consumed.with(Serdes.String(),ser));
        input.foreach((k, v) -> {
            System.out.println(k + " " + v.toString() + " "+ v);
        });

        input.to("out_stream", Produced.with(Serdes.String(), dser));

        Topology topology = builder.build(properties);
        topology.describe();
        streams = new KafkaStreams(topology, properties);
        streams.start();
    }
}
