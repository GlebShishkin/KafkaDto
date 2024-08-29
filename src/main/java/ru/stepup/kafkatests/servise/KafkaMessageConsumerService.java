package ru.stepup.kafkatests.servise;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import ru.stepup.kafkatests.dto.ConferenceDto;
import ru.stepup.kafkatests.dto.CustomRegDeserializer;
import ru.stepup.kafkatests.dto.RegistrationDto;
import ru.stepup.kafkatests.exceptions.NotFoundException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
@Service
public class KafkaMessageConsumerService {

	// вычитываем из "topic2" данные о заведении конференции через ConfController.addConference
	@SuppressWarnings({ "static-method", "unused" })
	@KafkaListener(topics = "topic2")
	public void onMessage(@Payload ConferenceDto msg,
						  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						  @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,	//???
						  @Header(KafkaHeaders.OFFSET) Long offset) {
		log.info("!!!!!!!!!!!!!!! Message consumed {}", msg);
	}

	// получение пользоватей в конференции id = conferenceID из "topic1"
	public String getNewRegisters (Integer conferenceID) {

		final String TOPIC = "topic1";
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomRegDeserializer.class);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroup");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		KafkaConsumer<String, RegistrationDto> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(TOPIC));
		ConsumerRecords<String, RegistrationDto> records = consumer.poll(Duration.ofMillis(10000L));
		log.info("2) ############## getNewRegisters: records.count() = " + records.count());

		JSONObject resultJson = new JSONObject();

		List<String> listNames = new ArrayList<>();
		for (ConsumerRecord<String, RegistrationDto> record : records) {
			log.info("############## Key: " + record.key() + ", Value: " + record.value());
			log.info("############## Partition: " + record.partition() + ", Offset:" + record.offset());
			if (record.value().conferenceId().longValue() == conferenceID) {
				listNames.add(record.value().name());
			}
		}
		consumer.close();

		if (listNames.size() == 0) {
			log.info("############## HttpStatus.NOT_FOUND");
			throw new NotFoundException("Conference " + conferenceID + " не найдена", HttpStatus.NOT_FOUND);
		}

		return resultJson.put("names",  new JSONArray(listNames)).toString();
	}
}