package ru.stepup.kafkatests.servise;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.stepup.kafkatests.dto.ConferenceDto;
import ru.stepup.kafkatests.dto.RegistrationDto;

@Slf4j
@Service
public class KafkaMessageProducerService {

    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    
	private final KafkaTemplate<String, RegistrationDto> kafkaRegTemplate;
    private final KafkaTemplate<String, ConferenceDto> kafkaConfTemplate;

    @Autowired
    public KafkaMessageProducerService(KafkaTemplate<String, RegistrationDto> kafkaRegTemplate, KafkaTemplate<String, ConferenceDto> kafkaConfTemplate) {
        this.kafkaRegTemplate = kafkaRegTemplate;
        this.kafkaConfTemplate = kafkaConfTemplate;
    }

    public void send(RegistrationDto msg) {
        log.info("1) ############## KafkaMessageProducerService");
        kafkaRegTemplate.send(TOPIC1, msg);
        log.info("2) ############## KafkaMessageProducerService");
    }

    public void addConf(ConferenceDto msg) {
        log.info("1) ############## KafkaMessageProducerService: sendConf");
        kafkaConfTemplate.send(TOPIC2, msg);
        log.info("2) ############## KafkaMessageProducerService: sendConf");
    }
}