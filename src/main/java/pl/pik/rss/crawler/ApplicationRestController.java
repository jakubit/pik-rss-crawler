package pl.pik.rss.crawler;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApplicationRestController {

    @GetMapping(name = "/")
    public ResponseEntity herokuStartupEndpoint() {
        String message = "[PIK RSS Crawler Service] This service will be running for 20 minutes from now. After this time, service will not be accessible and another request will be required.";
        return new ResponseEntity(message, HttpStatus.OK);
    }
}
