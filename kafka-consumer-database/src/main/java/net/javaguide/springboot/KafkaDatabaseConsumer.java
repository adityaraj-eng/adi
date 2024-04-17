package net.javaguide.springboot;

import net.javaguide.springboot.entity.WikimediaData;
import net.javaguide.springboot.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);


    private WikimediaDataRepository dataRepository;

    public KafkaDatabaseConsumer(WikimediaDataRepository wikimediaDataRepository){
        this.dataRepository = wikimediaDataRepository;
    }



    @KafkaListener(topics = "wikimedia_recentchange",groupId = "myGroup")
    public void consume(String eventMessage){
            LOGGER.info(String.format("Event Message received %s",eventMessage));

            WikimediaData wikimediaData = new WikimediaData();

            wikimediaData.setWikiData(eventMessage);
            LOGGER.error("line 31;;;;");
            dataRepository.save(wikimediaData);
        LOGGER.error("line 33;;;;");
    }
}
