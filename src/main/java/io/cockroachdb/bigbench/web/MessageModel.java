package io.cockroachdb.bigbench.web;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.springframework.hateoas.RepresentationModel;

@JsonPropertyOrder({"links"})
public class MessageModel extends RepresentationModel<MessageModel> {
    public static MessageModel from(String message) {
        return new MessageModel(message);
    }

    private String message;

    private String notice;

    private String cockroachVersion;

    public MessageModel() {
    }

    public MessageModel(String message) {
        this.message = message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getNotice() {
        return notice;
    }

    public void setNotice(String notice) {
        this.notice = notice;
    }

    public String getCockroachVersion() {
        return cockroachVersion;
    }

    public void setCockroachVersion(String cockroachVersion) {
        this.cockroachVersion = cockroachVersion;
    }
}
