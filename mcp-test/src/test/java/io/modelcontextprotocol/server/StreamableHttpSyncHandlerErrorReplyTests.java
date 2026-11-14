/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.server;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStreamableServerSession;
import io.modelcontextprotocol.spec.McpStreamableServerTransport;
import io.modelcontextprotocol.spec.McpStreamableServerTransportProvider;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the loss of the JSON-RPC reply a synchronous handler produces when
 * it throws.
 *
 * <p>
 * Synchronous handlers run as {@code Mono.fromCallable(...).subscribeOn(boundedElastic)}
 * ({@link McpServerFeatures.AsyncPromptSpecification#fromSync}), and their reply is
 * written on that worker ({@code McpStreamableServerSessionStream#handle} → flatMap
 * sendMessage). The transport subscribes with {@code block()} on the container thread,
 * which is also what {@code HttpServletStreamableServerTransportProvider} and the Spring
 * WebMVC transport do.
 *
 * <p>
 * When the callable throws, Reactor's {@code CallableSubscribeOnSubscription.run} cancels
 * its own subscription before delivering {@code onError}, and the late
 * {@code setMainFuture} on the subscribing thread then disposes the task it just
 * scheduled, with {@code Future.cancel(true)} (reactor-core issue 4483). If that
 * interrupt lands while the worker is still writing the error reply - which happens on
 * the worker that Reactor is not supposed to interrupt while it runs downstream work -
 * the flush fails and the client never gets the error reply.
 *
 * <p>
 * The transport used here simulates the interruptible flush of a real transport. The
 * flush window is widened to make the race observable in a test run; the window is
 * otherwise the only difference with the loss reported against the Spring WebMVC
 * transport.
 */
class StreamableHttpSyncHandlerErrorReplyTests {

	private static final int THREADS = 16;

	private static final int REQUESTS_PER_THREAD = 100;

	private static final Duration FLUSH_WINDOW = Duration.ofMillis(2);

	private static final String PROMPT_NAME = "boom";

	private static final Duration TIMEOUT = Duration.ofSeconds(20);

	@Test
	void errorReplyFromAThrowingSyncHandlerIsNotLost() throws Exception {
		Outcome outcome = run(THREADS, REQUESTS_PER_THREAD, FLUSH_WINDOW, true);
		System.out.println("error reply delivery: " + outcome);

		assertThat(outcome.lost()).as("lost error replies: %s", outcome).isZero();
		// The interrupt the scheduler delivers to a subscription whose callable threw is
		// the mechanism which aborts the write: it must not reach the worker at all
		assertThat(outcome.interruptedWrites()).as("writes interrupted by the scheduler: %s", outcome).isZero();
	}

	@Test
	void replyFromASucceedingSyncHandlerIsNotLost() throws Exception {
		Outcome outcome = run(THREADS, REQUESTS_PER_THREAD, FLUSH_WINDOW, false);
		System.out.println("reply delivery: " + outcome);

		assertThat(outcome.lost()).as("lost replies: %s", outcome).isZero();
	}

	private Outcome run(int threads, int requestsPerThread, Duration flushWindow, boolean handlerThrows)
			throws Exception {
		CapturingProvider provider = new CapturingProvider();
		McpServer.sync(provider)
			.serverInfo("test-server", "1.0.0")
			.capabilities(McpSchema.ServerCapabilities.builder().prompts(false).build())
			.prompts(new McpServerFeatures.SyncPromptSpecification(
					new McpSchema.Prompt(PROMPT_NAME, "A prompt whose handler may throw", List.of()),
					(exchange, request) -> {
						if (handlerThrows) {
							throw new IllegalStateException("handler failure");
						}
						return new McpSchema.GetPromptResult("ok", List.of());
					}))
			.build();

		McpStreamableServerSession session = provider.factory
			.startSession(new McpSchema.InitializeRequest("2025-06-18", McpSchema.ClientCapabilities.builder().build(),
					new McpSchema.Implementation("test-client", "1.0.0"), null))
			.session();

		Queue<McpSchema.JSONRPCMessage> sent = new ConcurrentLinkedQueue<>();
		AtomicInteger attempts = new AtomicInteger();
		AtomicInteger interruptedAtEntry = new AtomicInteger();
		AtomicInteger interruptedDuringFlush = new AtomicInteger();
		AtomicInteger failedSubscriptions = new AtomicInteger();

		ExecutorService executor = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		CountDownLatch done = new CountDownLatch(threads);
		for (int thread = 0; thread < threads; thread++) {
			int threadIndex = thread;
			executor.execute(() -> {
				try {
					start.await();
					for (int i = 0; i < requestsPerThread; i++) {
						FlushWindowTransport transport = new FlushWindowTransport(flushWindow, sent, attempts,
								interruptedAtEntry, interruptedDuringFlush);
						McpSchema.JSONRPCRequest request = new McpSchema.JSONRPCRequest(McpSchema.METHOD_PROMPT_GET,
								(threadIndex * requestsPerThread) + i + 1,
								Map.of("name", PROMPT_NAME, "arguments", Map.of()));
						try {
							session.responseStream(transport).handle(request).block(TIMEOUT);
						}
						catch (Exception ex) {
							failedSubscriptions.incrementAndGet();
						}
					}
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				finally {
					done.countDown();
				}
			});
		}
		start.countDown();
		assertThat(done.await(TIMEOUT.toSeconds() * 3, TimeUnit.SECONDS)).as("handler loop finished").isTrue();
		executor.shutdownNow();

		long replies = sent.stream().filter(McpSchema.JSONRPCResponse.class::isInstance).count();
		return new Outcome(threads * requestsPerThread, (int) replies, attempts.get(), interruptedAtEntry.get(),
				interruptedDuringFlush.get(), failedSubscriptions.get());
	}

	/**
	 * The outcome of one run, and the counters which tell apart the ways a reply can be
	 * lost.
	 *
	 * @param requests the number of requests the clients sent
	 * @param replies the number of JSON-RPC replies which reached the client
	 * @param writeAttempts the number of times the transport was asked to write
	 * @param interruptedAtEntry the writes which started on a thread whose interrupt flag
	 * was already set
	 * @param interruptedDuringFlush the writes whose flush was interrupted
	 * @param failedSubscriptions the requests whose subscription ended in an error
	 */
	record Outcome(int requests, int replies, int writeAttempts, int interruptedAtEntry, int interruptedDuringFlush,
			int failedSubscriptions) {

		int lost() {
			return this.requests - this.replies;
		}

		int interruptedWrites() {
			return this.interruptedAtEntry + this.interruptedDuringFlush;
		}

	}

	/**
	 * A transport whose flush is interruptible, like the one of a servlet or NIO
	 * transport: the interrupt status of the writing thread aborts the write.
	 */
	static class FlushWindowTransport implements McpStreamableServerTransport {

		private final Duration flushWindow;

		private final Queue<McpSchema.JSONRPCMessage> sent;

		private final AtomicInteger attempts;

		private final AtomicInteger interruptedAtEntry;

		private final AtomicInteger interruptedDuringFlush;

		FlushWindowTransport(Duration flushWindow, Queue<McpSchema.JSONRPCMessage> sent, AtomicInteger attempts,
				AtomicInteger interruptedAtEntry, AtomicInteger interruptedDuringFlush) {
			this.flushWindow = flushWindow;
			this.sent = sent;
			this.attempts = attempts;
			this.interruptedAtEntry = interruptedAtEntry;
			this.interruptedDuringFlush = interruptedDuringFlush;
		}

		@Override
		public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message, String messageId) {
			return Mono.fromRunnable(() -> {
				this.attempts.incrementAndGet();
				if (Thread.currentThread().isInterrupted()) {
					this.interruptedAtEntry.incrementAndGet();
					throw new IllegalStateException("The current thread was interrupted");
				}
				// The flush: the write of a real transport blocks, and aborts when the
				// thread is interrupted while it runs
				try {
					Thread.sleep(this.flushWindow.toMillis());
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					this.interruptedDuringFlush.incrementAndGet();
					throw new IllegalStateException("The current thread was interrupted", ex);
				}
				this.sent.add(message);
			});
		}

		@Override
		public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message) {
			return sendMessage(message, null);
		}

		@Override
		public Mono<Void> closeGracefully() {
			return Mono.empty();
		}

		@Override
		public void close() {
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> T unmarshalFrom(Object data, TypeRef<T> typeRef) {
			return (T) data;
		}

	}

	/**
	 * A provider which only records the session factory, so that a request can be fed to
	 * a stream directly, the way a transport provider feeds it after a POST.
	 */
	static class CapturingProvider implements McpStreamableServerTransportProvider {

		volatile McpStreamableServerSession.Factory factory;

		@Override
		public void setSessionFactory(McpStreamableServerSession.Factory sessionFactory) {
			this.factory = sessionFactory;
		}

		@Override
		public Mono<Void> notifyClients(String method, Object params) {
			return Mono.empty();
		}

		@Override
		public Mono<Void> closeGracefully() {
			return Mono.empty();
		}

	}

}