/*
 * Copyright 2026-2026 the original author or authors.
 */

package io.modelcontextprotocol.server;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import io.modelcontextprotocol.MockMcpServerTransport;
import io.modelcontextprotocol.MockMcpServerTransportProvider;
import io.modelcontextprotocol.common.McpTransportContext;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpStatelessServerTransport;
import io.modelcontextprotocol.spec.ProtocolVersions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Regression tests for structured output text fallback losing the result {@code _meta}.
 * When a tool returns only structured content, the server adds backwards-compatible
 * unstructured text content by rebuilding the {@link McpSchema.CallToolResult}; the
 * rebuild must preserve the metadata.
 */
class StructuredOutputMetaTests {

	private static final McpSchema.Implementation SERVER_INFO = McpSchema.Implementation.builder("test-server", "1.0.0")
		.build();

	private static final McpSchema.Implementation CLIENT_INFO = McpSchema.Implementation.builder("test-client", "1.0.0")
		.build();

	private static final Map<String, Object> METADATA = Map.of("example.com/trace-id", "trace-123");

	private static final McpSchema.Tool STRUCTURED_TOOL = McpSchema.Tool.builder("example", Map.of("type", "object"))
		.outputSchema(Map.of("type", "object"))
		.build();

	private static McpSchema.CallToolResult structuredResult() {
		return McpSchema.CallToolResult.builder().structuredContent(Map.of("value", 42)).meta(METADATA).build();
	}

	@Test
	void statelessServerTextFallbackPreservesMeta() {
		McpStatelessServerTransport transport = mock(McpStatelessServerTransport.class);

		McpServer.sync(transport)
			.serverInfo(SERVER_INFO)
			.toolCall(STRUCTURED_TOOL, (context, request) -> structuredResult())
			.build();

		ArgumentCaptor<McpStatelessServerHandler> handlerCaptor = ArgumentCaptor
			.forClass(McpStatelessServerHandler.class);
		verify(transport).setMcpHandler(handlerCaptor.capture());

		McpSchema.JSONRPCResponse response = handlerCaptor.getValue()
			.handleRequest(McpTransportContext.EMPTY,
					new McpSchema.JSONRPCRequest(McpSchema.METHOD_TOOLS_CALL, "1",
							McpSchema.CallToolRequest.builder("example").build()))
			.block(Duration.ofSeconds(5));

		assertThat(response).isNotNull();
		assertThat(response.error()).isNull();
		McpSchema.CallToolResult result = (McpSchema.CallToolResult) response.result();
		assertTextFallbackAndStructuredContent(result);
		assertThat(result.meta()).isEqualTo(METADATA);
	}

	@Test
	void statefulServerTextFallbackPreservesMeta() {
		MockMcpServerTransport transport = new MockMcpServerTransport();
		MockMcpServerTransportProvider transportProvider = new MockMcpServerTransportProvider(transport);

		McpServer.async(transportProvider)
			.serverInfo(SERVER_INFO)
			.toolCall(STRUCTURED_TOOL, (exchange, request) -> Mono.just(structuredResult()))
			.build();

		transportProvider.simulateIncomingMessage(initRequest());
		transportProvider.simulateIncomingMessage(initializedNotification());
		transport.clearSentMessages();

		String requestId = UUID.randomUUID().toString();
		transportProvider.simulateIncomingMessage(new McpSchema.JSONRPCRequest(McpSchema.METHOD_TOOLS_CALL, requestId,
				McpSchema.CallToolRequest.builder("example").build()));

		McpSchema.JSONRPCResponse response = awaitResponse(transport, requestId);
		assertThat(response.error()).isNull();
		McpSchema.CallToolResult result = (McpSchema.CallToolResult) response.result();
		assertTextFallbackAndStructuredContent(result);
		assertThat(result.meta()).isEqualTo(METADATA);
	}

	private static void assertTextFallbackAndStructuredContent(McpSchema.CallToolResult result) {
		assertThat(result.content()).hasSize(1);
		assertThat(result.content().get(0)).isInstanceOf(McpSchema.TextContent.class);
		assertThat(((McpSchema.TextContent) result.content().get(0)).text()).isEqualTo("{\"value\":42}");
		assertThat(result.structuredContent()).isEqualTo(Map.of("value", 42));
	}

	private static McpSchema.JSONRPCRequest initRequest() {
		return new McpSchema.JSONRPCRequest(McpSchema.METHOD_INITIALIZE, UUID.randomUUID().toString(),
				McpSchema.InitializeRequest
					.builder(ProtocolVersions.MCP_2025_11_25, McpSchema.ClientCapabilities.builder().build(),
							CLIENT_INFO)
					.build());
	}

	private static McpSchema.JSONRPCNotification initializedNotification() {
		return new McpSchema.JSONRPCNotification(McpSchema.METHOD_NOTIFICATION_INITIALIZED);
	}

	/**
	 * The mock transport delivers responses from {@code session.handle(...).subscribe()}
	 * without a completion signal, so wait briefly for the response with the given id.
	 */
	private static McpSchema.JSONRPCResponse awaitResponse(MockMcpServerTransport transport, Object requestId) {
		long deadline = System.currentTimeMillis() + 5000;
		while (System.currentTimeMillis() < deadline) {
			for (McpSchema.JSONRPCMessage message : transport.getAllSentMessages()) {
				if (message instanceof McpSchema.JSONRPCResponse response && requestId.equals(response.id())) {
					return response;
				}
			}
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted while waiting for response", ex);
			}
		}
		throw new AssertionError("No response with id " + requestId + " was sent");
	}

}
