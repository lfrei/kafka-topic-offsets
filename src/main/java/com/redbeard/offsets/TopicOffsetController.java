package com.redbeard.offsets;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TopicOffsetController {

    private final TopicOffsetConsumer consumer;

    public TopicOffsetController(TopicOffsetConsumer consumer) {
        this.consumer = consumer;
    }

    @GetMapping("/offset/{topic}")
    public Long getOffset(@PathVariable String topic) {
        return consumer.getOffset(topic);
    }
}
