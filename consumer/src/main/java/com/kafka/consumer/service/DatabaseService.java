package com.kafka.consumer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DatabaseService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void addAnswer(String answer) {

        String sql = "INSERT INTO exam (answer) VALUES (?)";
        jdbcTemplate.update(sql, answer);
    }
}
