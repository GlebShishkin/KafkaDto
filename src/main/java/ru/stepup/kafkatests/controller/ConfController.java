package ru.stepup.kafkatests.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.stepup.kafkatests.dto.ConferenceDto;
import ru.stepup.kafkatests.dto.RegistrationDto;
import ru.stepup.kafkatests.entity.ConfRegistration;
import ru.stepup.kafkatests.exceptions.NotFoundException;
import ru.stepup.kafkatests.servise.KafkaMessageConsumerService;
import ru.stepup.kafkatests.servise.KafkaMessageProducerService;

@Slf4j
@RestController
@RequestMapping("/api")
public class ConfController {

    private KafkaMessageProducerService kafkaMessageProducerService;
    private KafkaMessageConsumerService kafkaMessageConsumerService;

    @Autowired
    public void ProductController(KafkaMessageProducerService kafkaMessageProducerService
                                , KafkaMessageConsumerService kafkaMessageConsumerService
    ) {
        this.kafkaMessageProducerService = kafkaMessageProducerService;
        this.kafkaMessageConsumerService = kafkaMessageConsumerService;
    }

    // регистрация пользователя в конференции в "topic1"
    @PutMapping(value = "/register")
    public ResponseEntity<?> registrtion(@Valid @RequestBody RegistrationDto registrationDto) {

log.info("############## name = " + registrationDto.name() + "; getConferenceId  = " + registrationDto.conferenceId());
        try {
            kafkaMessageProducerService.send(registrationDto);
        } catch (Exception e) {
            throw new NotFoundException("Conference not found", HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(new ConfRegistration(registrationDto.name(), registrationDto.conferenceId()), HttpStatus.OK);
    }

    // получение пользоватей в конференции id = conferenceID из "topic1"
    @GetMapping("/getNewRegisters")
    public String getNewRegisters (@RequestParam Integer conferenceID) {

        log.info("1) ############## getNewRegisters: conferenceID = " + conferenceID);
        return kafkaMessageConsumerService.getNewRegisters(conferenceID);
    }

    // заведение конференции в "topic2"
    @PutMapping(value = "/addConference")
    public ResponseEntity<?> addConference(@Valid @RequestBody ConferenceDto conferenceDto) {
        try {
            kafkaMessageProducerService.addConf(conferenceDto);
        }
        catch (Exception e) {
            ResponseEntity.badRequest().body("Conferece exist");
        }
        return ResponseEntity.ok().body("");
    }
}


