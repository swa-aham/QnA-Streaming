package com.kafka.consumer.entity;

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

    public QnA(Long id, String question, String answer) {
        this.id = id;
        this.question = question;
        this.answer = answer;
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

    @Override
    public String toString() {
        return "QnA{" +
                "id=" + id +
                ", question='" + question + '\'' +
                ", answer='" + answer + '\'' +
                '}';
    }
}
