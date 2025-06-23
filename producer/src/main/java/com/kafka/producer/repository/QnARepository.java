package com.kafka.producer.repository;

import com.kafka.producer.entity.QnA;
import org.springframework.data.jpa.repository.JpaRepository;

public interface QnARepository extends JpaRepository<QnA, Long> {
}
