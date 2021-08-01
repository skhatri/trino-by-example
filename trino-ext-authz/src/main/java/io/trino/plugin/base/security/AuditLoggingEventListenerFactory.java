package io.trino.plugin.base.security;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryFailureInfo;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class AuditLoggingEventListenerFactory implements EventListenerFactory {
    @Override
    public String getName() {
        return "auditlog";
    }

    @Override
    public EventListener create(Map<String, String> config) {
        System.out.println("event listener input " + config);
        return new LoggingEventListener();
    }
}

class AttributeLogger {
    private Map<String, Object> attributes = new TreeMap<>();

    private AttributeLogger() {
    }

    public AttributeLogger withAttribute(String name, Object value) {
        this.attributes.put(name, value);
        return this;
    }

    public void log() {
        List<String> snippets = new ArrayList<>();
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            snippets.add("\"".concat(entry.getKey()).concat("\":\"").concat(entry.getValue().toString()).concat("\""));
        }
        System.out.println(snippets.stream().collect(Collectors.joining(",", "{", "}")));
    }

    public static AttributeLogger newInstance() {
        return new AttributeLogger();
    }
}

class LoggingEventListener implements EventListener {
    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        AttributeLogger.newInstance()
            .withAttribute("phase", "created")
            .withAttribute("query", queryCreatedEvent.getMetadata().getQuery())
            .withAttribute("query_id", queryCreatedEvent.getMetadata().getQueryId())
            .withAttribute("catalog", queryCreatedEvent.getContext().getCatalog().orElse(""))
            .withAttribute("schema", queryCreatedEvent.getContext().getSchema().orElse(""))
            .withAttribute("principal", queryCreatedEvent.getContext().getPrincipal().orElse(""))
            .withAttribute("user", queryCreatedEvent.getContext().getUser())
            .withAttribute("time", queryCreatedEvent.getCreateTime().atZone(ZoneId.systemDefault()))
            .log();
    }


    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        AttributeLogger builder = AttributeLogger.newInstance()
            .withAttribute("phase", "completed")
            .withAttribute("query", queryCompletedEvent.getMetadata().getQuery())
            .withAttribute("query_id", queryCompletedEvent.getMetadata().getQueryId())
            .withAttribute("catalog", queryCompletedEvent.getContext().getCatalog().orElse(""))
            .withAttribute("schema", queryCompletedEvent.getContext().getSchema().orElse(""))
            .withAttribute("principal", queryCompletedEvent.getContext().getPrincipal().orElse(""))
            .withAttribute("user", queryCompletedEvent.getContext().getUser())
            .withAttribute("time", queryCompletedEvent.getCreateTime().atZone(ZoneId.systemDefault()))
            .withAttribute("execution_time", queryCompletedEvent.getStatistics().getExecutionTime().orElse(Duration.ZERO).toMillis())
            .withAttribute("cpu_time", queryCompletedEvent.getStatistics().getCpuTime().toMillis())
            .withAttribute("analysis_time", queryCompletedEvent.getStatistics().getAnalysisTime().orElse(Duration.ZERO).toMillis())
            .withAttribute("rows", queryCompletedEvent.getStatistics().getOutputRows());

            if (queryCompletedEvent.getFailureInfo().isPresent()) {
                QueryFailureInfo failure = queryCompletedEvent.getFailureInfo().get();
                builder.withAttribute("error_code", failure.getErrorCode().getName());
                builder.withAttribute("message", failure.getFailureMessage().orElse(""));
                builder.withAttribute("failure_type", failure.getFailureType().orElse(""));
                builder.withAttribute("failure_host", failure.getFailureHost().orElse(""));
                builder.withAttribute("status", "failure");
            } else {
                builder.withAttribute("status", "success");
            }

            builder.log();
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {

    }
}