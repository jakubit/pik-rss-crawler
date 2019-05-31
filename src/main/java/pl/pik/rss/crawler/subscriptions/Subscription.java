package pl.pik.rss.crawler.subscriptions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "subscriptions")
@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class Subscription {

    @Id
    private String url;
    private LocalDateTime lastUpdate;
}

