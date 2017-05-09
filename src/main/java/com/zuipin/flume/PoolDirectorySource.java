package com.zuipin.flume;

import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BATCH_SIZE;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BUFFER_MAX_LINE_LENGTH;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.CONSUME_ORDER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DECODE_ERROR_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_BASENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_DELETE_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_DESERIALIZER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_FILE_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_IGNORE_PAT;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_INCLUDE_PAT;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_MAX_BACKOFF;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_POLL_DELAY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_RECURSIVE_DIRECTORY_SEARCH;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_SPOOLED_FILE_SUFFIX;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DEFAULT_TRACKER_DIR;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DELETE_POLICY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.DESERIALIZER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.IGNORE_PAT;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.INCLUDE_PAT;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.INPUT_CHARSET;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.MAX_BACKOFF;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.POLL_DELAY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.RECURSIVE_DIRECTORY_SEARCH;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.SPOOLED_FILE_SUFFIX;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.TRACKER_DIR;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.ConsumeOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * @Title: PoolDirectorySource
 * @Package: com.zuipin.flume
 * @author: zengxinchao
 * @date: 2017年1月19日 下午5:31:56
 * @Description: 监测一个目录的新增文件，监测周期为一天，同一时间 只能监测一个文件
 */
public class PoolDirectorySource extends AbstractSource implements Configurable, EventDrivenSource {
	
	private static final Logger logger = LoggerFactory.getLogger(PoolDirectorySource.class);
	
	/* Config options */
	private String completedSuffix;
	private String spoolDirectory;
	private boolean fileHeader;
	private String fileHeaderKey;
	private boolean basenameHeader;
	private String basenameHeaderKey;
	private int batchSize;
	private String includePattern;
	private String ignorePattern;
	private String trackerDirPath;
	private String deserializerType;
	private Context deserializerContext;
	private String deletePolicy;
	private String inputCharset;
	private DecodeErrorPolicy decodeErrorPolicy;
	private volatile boolean hasFatalError = false;
	
	private SourceCounter sourceCounter;
	ReliableSpoolingFileEventReader reader;
	private ScheduledExecutorService executor;
	private boolean backoff = true;
	private boolean hitChannelException = false;
	private boolean hitChannelFullException	= false;
	private int maxBackoff;
	private ConsumeOrder consumeOrder;
	private int pollDelay;
	private boolean recursiveDirectorySearch;
	
	@Override
	public synchronized void start() {
		logger.info("SpoolDirectorySource source starting with directory: {}", spoolDirectory);
		
		executor = Executors.newSingleThreadScheduledExecutor();
		
		File directory = new File(spoolDirectory);
		try {
			reader = new ReliableSpoolingFileEventReader.Builder().spoolDirectory(directory).completedSuffix(completedSuffix).includePattern(includePattern)
					.ignorePattern(ignorePattern).trackerDirPath(trackerDirPath).annotateFileName(fileHeader).fileNameHeader(fileHeaderKey).annotateBaseName(basenameHeader)
					.baseNameHeader(basenameHeaderKey).deserializerType(deserializerType).deserializerContext(deserializerContext).deletePolicy(deletePolicy)
					.inputCharset(inputCharset).decodeErrorPolicy(decodeErrorPolicy).consumeOrder(consumeOrder).recursiveDirectorySearch(recursiveDirectorySearch).build();
		} catch (IOException ioe) {
			throw new FlumeException("Error instantiating spooling event parser", ioe);
		}
		
		Runnable runner = new SpoolDirectoryRunnable(reader, sourceCounter);
		executor.scheduleWithFixedDelay(runner, 0, pollDelay, TimeUnit.MILLISECONDS);
		
		super.start();
		logger.debug("SpoolDirectorySource source started");
		sourceCounter.start();
	}
	
	@Override
	public synchronized void stop() {
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			logger.info("Interrupted while awaiting termination", ex);
		}
		executor.shutdownNow();
		
		super.stop();
		sourceCounter.stop();
		logger.info("SpoolDir source {} stopped. Metrics: {}", getName(), sourceCounter);
	}
	
	@Override
	public String toString() {
		return "Spool Directory source " + getName() + ": { spoolDir: " + spoolDirectory + " }";
	}
	
	public synchronized void configure(Context context) {
		spoolDirectory = context.getString(SPOOL_DIRECTORY);
		Preconditions.checkState(spoolDirectory != null, "Configuration must specify a spooling directory");
		
		completedSuffix = context.getString(SPOOLED_FILE_SUFFIX, DEFAULT_SPOOLED_FILE_SUFFIX);
		deletePolicy = context.getString(DELETE_POLICY, DEFAULT_DELETE_POLICY);
		fileHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILE_HEADER);
		fileHeaderKey = context.getString(FILENAME_HEADER_KEY, DEFAULT_FILENAME_HEADER_KEY);
		basenameHeader = context.getBoolean(BASENAME_HEADER, DEFAULT_BASENAME_HEADER);
		basenameHeaderKey = context.getString(BASENAME_HEADER_KEY, DEFAULT_BASENAME_HEADER_KEY);
		batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
		inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
		decodeErrorPolicy = DecodeErrorPolicy.valueOf(context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY).toUpperCase(Locale.ENGLISH));
		
		includePattern = context.getString(INCLUDE_PAT, DEFAULT_INCLUDE_PAT);
		ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
		trackerDirPath = context.getString(TRACKER_DIR, DEFAULT_TRACKER_DIR);
		
		deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
		deserializerContext = new Context(context.getSubProperties(DESERIALIZER + "."));
		
		consumeOrder = ConsumeOrder.valueOf(context.getString(CONSUME_ORDER, DEFAULT_CONSUME_ORDER.toString()).toUpperCase(Locale.ENGLISH));
		
		pollDelay = context.getInteger(POLL_DELAY, DEFAULT_POLL_DELAY);
		
		recursiveDirectorySearch = context.getBoolean(RECURSIVE_DIRECTORY_SEARCH, DEFAULT_RECURSIVE_DIRECTORY_SEARCH);
		
		// "Hack" to support backwards compatibility with previous generation of
		// spooling directory source, which did not support deserializers
		@SuppressWarnings("deprecation")
		Integer bufferMaxLineLength = context.getInteger(BUFFER_MAX_LINE_LENGTH);
		if (bufferMaxLineLength != null && deserializerType != null && deserializerType.equalsIgnoreCase(DEFAULT_DESERIALIZER)) {
			deserializerContext.put(LineDeserializer.MAXLINE_KEY, bufferMaxLineLength.toString());
		}
		
		maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}
	
	@VisibleForTesting
	protected boolean hasFatalError() {
		return hasFatalError;
	}
	
	/**
	 * The class always backs off, this exists only so that we can test without taking a really long time.
	 *
	 * @param backoff
	 *            - whether the source should backoff if the channel is full
	 */
	@VisibleForTesting
	protected void setBackOff(boolean backoff) {
		this.backoff = backoff;
	}
	
	@VisibleForTesting
	protected boolean didHitChannelException() {
		return hitChannelException;
	}
	
	@VisibleForTesting
	protected boolean didHitChannelFullException() {
		return hitChannelFullException;
	}
	
	@VisibleForTesting
	protected SourceCounter getSourceCounter() {
		return sourceCounter;
	}
	
	@VisibleForTesting
	protected boolean getRecursiveDirectorySearch() {
		return recursiveDirectorySearch;
	}
	
	private class SpoolDirectoryRunnable implements Runnable {
		private ReliableSpoolingFileEventReader	reader;
		private SourceCounter					sourceCounter;
		
		public SpoolDirectoryRunnable(ReliableSpoolingFileEventReader reader, SourceCounter sourceCounter) {
			this.reader = reader;
			this.sourceCounter = sourceCounter;
		}
		
		public void run() {
			int backoffInterval = 250;
			try {
				while (!Thread.interrupted()) {
					List<Event> events = reader.readEvents(batchSize);
					if (events.isEmpty()) {
						break;
					}
					
					sourceCounter.addToEventReceivedCount(events.size());
					sourceCounter.incrementAppendBatchReceivedCount();
					
					try {
						getChannelProcessor().processEventBatch(events);
						reader.commit();
					} catch (ChannelFullException ex) {
						logger.warn("The channel is full, and cannot write data now. The " + "source will try again after " + backoffInterval + " milliseconds");
						hitChannelFullException = true;
						backoffInterval = waitAndGetNewBackoffInterval(backoffInterval);
						continue;
					} catch (ChannelException ex) {
						logger.warn("The channel threw an exception, and cannot write data now. The " + "source will try again after " + backoffInterval + " milliseconds");
						hitChannelException = true;
						backoffInterval = waitAndGetNewBackoffInterval(backoffInterval);
						continue;
					}
					backoffInterval = 250;
					sourceCounter.addToEventAcceptedCount(events.size());
					sourceCounter.incrementAppendBatchAcceptedCount();
				}
			} catch (Throwable t) {
				logger.error("FATAL: " + PoolDirectorySource.this.toString() + ": " + "Uncaught exception in SpoolDirectorySource thread. "
						+ "Restart or reconfigure Flume to continue processing.", t);
				hasFatalError = true;
				Throwables.propagate(t);
			}
		}
		
		private int waitAndGetNewBackoffInterval(int backoffInterval) throws InterruptedException {
			if (backoff) {
				TimeUnit.MILLISECONDS.sleep(backoffInterval);
				backoffInterval = backoffInterval << 1;
				backoffInterval = backoffInterval >= maxBackoff ? maxBackoff : backoffInterval;
			}
			return backoffInterval;
		}
	}
}
