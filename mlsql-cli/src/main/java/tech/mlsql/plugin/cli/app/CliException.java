package tech.mlsql.plugin.cli.app;

import java.util.ArrayList;
import java.util.List;

/**
 * 25/8/2021 WilliamZhu(allwefantasy@gmail.com)
 */
public class CliException extends RuntimeException {
    private List<String> detailedMessages = new ArrayList<>();

    public List<String> getDetailedMessages() {
        return detailedMessages;
    }

    void addMessage(String message) {
        detailedMessages.add(message);
    }

    public List<String> getMessages() {
        return detailedMessages;
    }
}


