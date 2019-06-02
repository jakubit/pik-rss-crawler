package pl.pik.rss.crawler.subscriptions.repository;

import org.springframework.data.repository.CrudRepository;
import pl.pik.rss.crawler.subscriptions.model.Subscription;

public interface SubscriptionRepository extends CrudRepository<Subscription, String> {

}
