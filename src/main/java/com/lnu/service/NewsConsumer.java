package com.lnu.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lnu.model.News;
import com.lnu.repository.NewsRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NewsConsumer {

  @Autowired
  NewsRepository newsRepository;

  @KafkaListener(topics = {"pravda"}, groupId = "mygroup")
  public void consumeFromTopic(String news) throws JsonProcessingException {
    System.out.println("Consumed news " + news);
    News gottenNews = new ObjectMapper().readValue(news, News.class);
		newsRepository.save(gottenNews);
		System.out.println("Received news saved to db");
	}
}
