package pl.pik.rss.crawler.crawler;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import pl.pik.rss.crawler.kafka.KafkaMessageProducer;
import pl.pik.rss.crawler.kafka.RssChannelInfo;
import pl.pik.rss.crawler.subscriptions.model.Subscription;
import pl.pik.rss.crawler.subscriptions.repository.SubscriptionRepository;

import java.io.IOException;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class RssCrawler {
    private SyndFeedInput syndFeedInput;
    private KafkaMessageProducer kafkaMessageProducer;
    private SubscriptionRepository subscriptionRepository;

    @Autowired
    public RssCrawler(SubscriptionRepository subscriptionRepository, KafkaMessageProducer kafkaMessageProducer) {
        this.subscriptionRepository = subscriptionRepository;
        this.kafkaMessageProducer = kafkaMessageProducer;
        this.syndFeedInput = new SyndFeedInput();
    }

    @Scheduled(fixedDelay = 10000)
    public void crawlOverSubscriptions() throws IOException, FeedException {
        Iterable<Subscription> subscriptions = subscriptionRepository.findAll();

        for (Subscription subscription : subscriptions) {
            String subscriptionUrl = subscription.getUrl();
            
            LocalDateTime lastUpdate = subscription.getLastUpdate();
            setLastUpdateDateOfSubscription(subscriptionUrl);

            SyndFeed feed = fetchFeed(subscriptionUrl);
            List<SyndEntry> newEntries = fetchNewEntries(feed, lastUpdate);
            publishFeedEntriesToKafka(feed, newEntries, subscriptionUrl);
        }
    }

    private void publishFeedEntriesToKafka(SyndFeed feed, List<SyndEntry> newEntries, String subscriptionUrl) {
        RssChannelInfo rssChannelInfo = new RssChannelInfo(feed.getTitle(), feed.getDescription(), feed.getLanguage(), subscriptionUrl);

        for (SyndEntry entry : newEntries) {
            kafkaMessageProducer.produce(rssChannelInfo, entry);
        }
    }

    private void setLastUpdateDateOfSubscription(String subscriptionUrl) {
        Subscription updatedSubscription = new Subscription(subscriptionUrl, LocalDateTime.now());
        subscriptionRepository.save(updatedSubscription);
    }

    private SyndFeed fetchFeed(String url) throws IOException, FeedException {
        URL feedSource = new URL(url);
        return syndFeedInput.build(new XmlReader(feedSource));
    }

    private List<SyndEntry> fetchNewEntries(SyndFeed feed, LocalDateTime lastUpdate) {
        List<SyndEntry> entries = feed.getEntries();
        return filterEntries(feed.getLink(), entries, lastUpdate);
    }

    private List<SyndEntry> filterEntries(String channelUrl, List<SyndEntry> allNews, LocalDateTime lastUpdate) {
        return allNews.stream().filter(
                (entry -> isNewsNew(channelUrl, entry, lastUpdate))
        ).collect(Collectors.toList());
    }

    private static boolean isNewsNew(String channelUrl, SyndEntry entry, LocalDateTime lastUpdate) {
        LocalDateTime entryPublishDate = convertToLocalDateTimeViaSqlTimestamp(entry.getPublishedDate());
        System.out.println(String.format("\n\nCHANNEL: %s\nTITLE: %s \nDATE: %s \nLAST_UPDATE: %s", channelUrl, entry.getTitle(), entry.getPublishedDate(), lastUpdate));
        if (entryPublishDate.isAfter(lastUpdate)) {
            System.out.println("IS NEW");
            return true;
        } else {
            System.out.println("IS NOT NEW");
            return false;
        }
    }

    private static LocalDateTime convertToLocalDateTimeViaSqlTimestamp(Date dateToConvert) {
        return new java.sql.Timestamp(
                dateToConvert.getTime()).toLocalDateTime();
    }

}
