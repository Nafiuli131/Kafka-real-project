package com.example.config;

import com.example.entity.WikimediaData;
import com.example.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    @Autowired
    WikimediaDataRepository wikimediaDataRepository;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(
            topics = "wikimedia-recentChange",
            groupId = "myGroup"
    )
    public void receiveMessage(String message){
        LOGGER.info(String.format("Event message received -> %s", message));
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikiEventData(message);
        wikimediaDataRepository.save(wikimediaData);
    }

}
