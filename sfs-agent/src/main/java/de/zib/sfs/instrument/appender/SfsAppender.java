/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.appender;

import java.io.Serializable;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

/**
 * Appender that aggregates log events before writing them to CSV.
 * 
 * @author robert
 *
 */
@Plugin(name = "Sfs", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class SfsAppender extends AbstractAppender {

    private final SfsLogEventAggregator localAggregation;

    private SfsAppender(String name, Filter filter,
            Layout<? extends Serializable> layout, long timeBinDuration,
            int timeBinCacheSize, String outputDirectory, String outputSeparator) {
        super(name, filter, layout);
        localAggregation = new SfsLogEventAggregator(timeBinDuration,
                timeBinCacheSize, outputDirectory, outputSeparator);
    }

    @Override
    public void start() {
        localAggregation.start();
        super.start();
    }

    @Override
    public void stop() {
        try {
            localAggregation.stop(0);
        } catch (InterruptedException e) {
            LOGGER.error("Could not stop local aggregation", e);
        }
        super.stop();
    }

    @Override
    public void append(LogEvent event) {
        // simply forward all events to the aggregator
        localAggregation.append(event.getMessage().getFormattedMessage());
    }

    public static class Builder<B extends Builder<B>> extends
            AbstractAppender.Builder<B> implements
            org.apache.logging.log4j.core.util.Builder<SfsAppender> {

        public Builder() {
            super();
            withTimeBinDuration(-1);
            withTimeBinCacheSize(-1);
            withOutputDirectory(null);
            withOutputSeparator(null);
        }

        @PluginAttribute("timeBinDuration")
        private long timeBinDuration;

        @PluginAttribute("timeBinCacheSize")
        private int timeBinCacheSize;

        @PluginAttribute("outputDirectory")
        private String outputDirectory;

        @PluginAttribute("outputSeparator")
        private String outputSeparator;

        @Override
        public SfsAppender build() {
            if (timeBinDuration < 0) {
                LOGGER.error("No valid time bin duration provided for SfsAppender with name "
                        + getName());
                return null;
            }

            if (timeBinCacheSize < 0) {
                LOGGER.error("No valid time bin cache size provided for SfsAppender with name "
                        + getName());
                return null;
            }

            if (outputDirectory == null) {
                LOGGER.error("No valid output directory provided for SfsAppender with name "
                        + getName());
                return null;
            }

            if (outputSeparator == null) {
                LOGGER.error("No valid output separator provided for SfsAppender with name "
                        + getName());
                return null;
            }

            return new SfsAppender(getName(), getFilter(), getLayout(),
                    timeBinDuration, timeBinCacheSize, outputDirectory,
                    outputSeparator);
        }

        public B withTimeBinDuration(long timeBinDuration) {
            this.timeBinDuration = timeBinDuration;
            return asBuilder();
        }

        public B withTimeBinCacheSize(int timeBinCacheSize) {
            this.timeBinCacheSize = timeBinCacheSize;
            return asBuilder();
        }

        public B withOutputDirectory(String outputDirectory) {
            this.outputDirectory = outputDirectory;
            return asBuilder();
        }

        public B withOutputSeparator(String outputSeparator) {
            this.outputSeparator = outputSeparator;
            return asBuilder();
        }

    }

    @PluginBuilderFactory
    public static <B extends Builder<B>> B newBuilder() {
        return new Builder<B>().asBuilder();
    }

}
