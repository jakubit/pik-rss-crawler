package pl.pik.rss.crawler.subscriptions.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import pl.pik.rss.crawler.subscriptions.Subscription;
import pl.pik.rss.crawler.subscriptions.SubscriptionRepository;


import java.time.LocalDateTime;
import java.util.Optional;

import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
public class SubscriptionController {
    private SubscriptionRepository subscriptionRepository;

    @Autowired
    public SubscriptionController(SubscriptionRepository subscriptionRepository) {
        this.subscriptionRepository = subscriptionRepository;
    }

    @RequestMapping(value = "/subscribe", params = "rssUrl", method = POST)
    public void subscribe(@RequestParam("rssUrl") String rssUrl) {
        Subscription subscription = new Subscription(rssUrl, LocalDateTime.now().minusDays(1));
        subscriptionRepository.save(subscription);
    }

    @RequestMapping(value = "/subscription", params = "rssUrl", method = GET)
    public Optional<Subscription> getSubscription(@RequestParam("rssUrl") String rssUrl) {
        return subscriptionRepository.findById(rssUrl);
    }
}
