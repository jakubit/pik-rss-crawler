package pl.pik.rss.crawler.kafka;

import com.rometools.rome.feed.synd.SyndEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaMessageProducer {
    private static final String KAFKA_BROKERS = "52.169.28.113:9092, 52.169.28.113:9093";
    private static final String CLIENT_ID = "rssCrawler";
    private static final String TOPIC_NAME = "rawMessages";

    private Producer<RssChannelInfo, SyndEntry> producer;

    public KafkaMessageProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaMessageSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaMessageSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    public void produce(RssChannelInfo rssChannelInfo, SyndEntry entry) {
        producer.send(new ProducerRecord<>(TOPIC_NAME, rssChannelInfo, entry));
    }

}
