package pl.pik.rss.crawler.kafka;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
@Data
public class RssChannelInfo {
    private String title;
    private String description;
    private String language;
    private String url;

    public RssChannelInfo(String title, String description, String language, String url) {
        this.title = title;
        this.description = description;
        this.language = language;
        this.url = url;
    }
}
