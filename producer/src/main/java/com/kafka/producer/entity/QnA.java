package com.kafka.producer.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "exam")
public class QnA {

    @Id
    private Long id;

    private String question;

    private String answer;

    public QnA(){}

    public QnA(Long id, String question) {
        this.id = id;
        this.question = question;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getAnswer() {
        return answer;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }
}
