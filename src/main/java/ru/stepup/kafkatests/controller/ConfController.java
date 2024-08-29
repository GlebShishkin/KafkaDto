package ru.stepup.kafkatests.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;
import ru.stepup.kafkatests.dto.ConferenceDto;
import ru.stepup.kafkatests.dto.RegistrationDto;
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
    public ResponseEntity<?> registrtion(@Valid @RequestBody RegistrationDto registrationDto,
                                      BindingResult bindingResult) throws BindException {

log.info("############## name = " + registrationDto.name() + "; getConferenceId  = " + registrationDto.conferenceId());
        if (bindingResult.hasErrors()) {
            if (bindingResult instanceof BindException exception) {
                throw exception;
            } else {
                throw new BindException(bindingResult);
            }
        } else {
            kafkaMessageProducerService.send(registrationDto);
            return ResponseEntity.noContent()
                    .build();
        }
    }

    // получение пользоватей в конференции id = conferenceID из "topic1"
    @GetMapping("/getNewRegisters")
    public String getNewRegisters (@RequestParam Integer conferenceID) {

        log.info("1) ############## getNewRegisters: conferenceID = " + conferenceID);
        return kafkaMessageConsumerService.getNewRegisters(conferenceID);
    }

    // заведение конференции в "topic2"
    @PutMapping(value = "/addConference")
    public ResponseEntity<?> addConference(@Valid @RequestBody ConferenceDto conferenceDto,
                                         BindingResult bindingResult) throws BindException {

        log.info("############## getConferenceId  = " + conferenceDto.conferenceId() + "; name = " + conferenceDto.name());
        if (bindingResult.hasErrors()) {
            if (bindingResult instanceof BindException exception) {
                throw exception;
            } else {
                throw new BindException(bindingResult);
            }
        } else {
            kafkaMessageProducerService.addConf(conferenceDto);
            return ResponseEntity.noContent()
                    .build();
        }
    }
}


