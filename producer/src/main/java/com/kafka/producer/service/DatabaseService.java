package com.kafka.producer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DatabaseService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void addQuestion(String question) {

        String sql = "INSERT INTO exam (question) VALUES (?)";
        jdbcTemplate.update(sql, question);
    }
}
