package io.wizzie.normalizer.builder;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class StreamMonitor extends Thread {
    private static final Logger log = LoggerFactory.getLogger(StreamMonitor.class);

    KafkaStreams streams;
    Builder builder;
    boolean isMonitoring = false;

    public StreamMonitor(Builder builder) {
        this.streams = builder.streams;
        this.builder = builder;
    }

    public void close() {
        log.info("Stopping Stream Monitor thread");
        isMonitoring = false;
        this.interrupt();
    }

    @Override
    public void run() {
        log.info("Start Stream Monitor thread.");
        isMonitoring = true;
        while (isMonitoring) {
            if (streams.state().isRunning()) {
                List<Boolean> streamsRunning = streams.localThreadsMetadata()
                        .stream()
                        .map(x -> !x.threadState().equals(StreamThread.State.DEAD.name()))
                        .collect(Collectors.toList());

                log.debug("Check stream threads are running: {}", streamsRunning.toString());
                if (!streamsRunning.contains(true)) {
                    log.info("Detect streams is shutdown, I'm going to close the builder.");
                    try {
                        builder.close();
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }

                    isMonitoring = false;
                } else {
                    try {
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                    }
                }
            }else {
                try {
                    Thread.sleep(60000);
                    log.info("Streams not running. Stream Monitor sleeping.");
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        log.info("Shutdown Stream Monitor thread.");
    }
}
