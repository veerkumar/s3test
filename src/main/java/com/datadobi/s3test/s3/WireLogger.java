package com.datadobi.s3test.s3;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.junit.runner.Description;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class WireLogger {
    static {
        ConfigurationBuilder<BuiltConfiguration> configBuilder =
                ConfigurationBuilderFactory.newConfigurationBuilder();
        Configurator.initialize(configBuilder
                .add(configBuilder.newRootLogger(Level.OFF))
                .add(configBuilder.newLogger("org.apache.http.wire")
                        .addAttribute("level", Level.DEBUG))
                .build(false));
    }

    private final @Nullable Path logPath;
    private @Nullable Configuration previousConfiguration;

    public WireLogger(@Nullable Path logPath) {
        this.logPath = logPath;
    }

    private @Nullable Path logPath(Description description) throws IOException {
        if (logPath == null) {
            return null;
        }

        Path path = logPath.resolve(description.getTestClass().getSimpleName()).resolve(description.getMethodName());
        return Files.createDirectories(path);
    }

    public void start(Description description) throws IOException {
        Path path = logPath(description);
        if (path == null) {
            return;
        }

        Path logFile = path.resolve("wire.log");
        Files.deleteIfExists(logFile);

        ConfigurationBuilder<BuiltConfiguration> configBuilder =
                ConfigurationBuilderFactory.newConfigurationBuilder();
        Configuration configuration = configBuilder
                .add(configBuilder
                        .newAppender("wire", "File")
                        .addAttribute("fileName", logFile)
                        .add(configBuilder.newLayout("PatternLayout").addAttribute("pattern", "%m%n")))
                .add(configBuilder.newRootLogger(Level.OFF))
                .add(configBuilder.newLogger("org.apache.http.wire")
                        .addAttribute("level", Level.DEBUG)
                        .add(configBuilder.newAppenderRef("wire")))
                .build(false);

        previousConfiguration = LoggerContext.getContext().getConfiguration();
        Configurator.reconfigure(configuration);
    }

    public void stop() {
        if (previousConfiguration != null) {
            Configurator.reconfigure(previousConfiguration);
        }
    }
}
