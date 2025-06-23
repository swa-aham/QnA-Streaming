package com.kafka.consumer.repository;

import com.kafka.consumer.entity.QnA;
import org.springframework.data.jpa.repository.JpaRepository;

public interface QnARepository extends JpaRepository<QnA, Long> {
}
