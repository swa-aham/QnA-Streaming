package com.kafka.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DatabaseService {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void addAnswer(String answer) {
        logger.debug("Adding answer to database: {}", answer);
        try {
            String sql = "INSERT INTO exam (answer) VALUES (?)";
            jdbcTemplate.update(sql, answer);
            logger.info("Successfully added answer to database");
        } catch (Exception e) {
            logger.error("Failed to add answer to database: {}", answer, e);
            throw e;
        }
    }
}
