package mcp

// OutboundTools returns the Gemini function-calling tool definitions for the
// partner services Kineticz integrates with. Gemini 3.5 Flash is expected to
// call Elastic and Dynatrace in parallel during the diagnose stage; Arize
// Phoenix is queried separately for historical trace context.
func OutboundTools() []ToolDefinition {
	return []ToolDefinition{
		elasticLookupContractTool(),
		dynatraceQueryConsumerHealthTool(),
		arizePhoenixQueryTracesTool(),
	}
}

func elasticLookupContractTool() ToolDefinition {
	return ToolDefinition{
		Name:        "elastic_lookup_contract",
		Description: "Fetch the YAML contract definition for a named contract and retrieve the top three historical mitigation patterns. Combines BM25 column matching with KNN diff-embedding similarity under Reciprocal Rank Fusion (rank_constant=60).",
		Parameters: Schema{
			Type: "OBJECT",
			Properties: map[string]Schema{
				"contract_name": {
					Type:        "STRING",
					Description: "Identifier of the contract (for example, users_v1).",
				},
				"columns": {
					Type:        "ARRAY",
					Description: "Column names from the failing pipeline. Used as the BM25 query against columns and table_metadata.",
					Items:       &Schema{Type: "STRING"},
				},
				"diff_embedding": {
					Type:        "ARRAY",
					Description: "Embedding vector of the current pipeline diff. Used as the KNN query against historical diff_embedding vectors.",
					Items:       &Schema{Type: "NUMBER"},
				},
			},
			Required: []string{"contract_name", "columns", "diff_embedding"},
		},
	}
}

func arizePhoenixQueryTracesTool() ToolDefinition {
	return ToolDefinition{
		Name:        "arize_phoenix_query_traces",
		Description: "Query Arize Phoenix for spans matching a correlation token or anomaly, returning prior repair attempts, their verdicts, and Gemini reasoning blocks. Backed by the Phoenix MCP server (@arizeai/phoenix-mcp).",
		Parameters: Schema{
			Type: "OBJECT",
			Properties: map[string]Schema{
				"correlation_token": {
					Type:        "STRING",
					Description: "Token threaded through the Kineticz pipeline; matches the span attribute kineticz.correlation_token.",
				},
				"lookback_minutes": {
					Type:        "INTEGER",
					Description: "How far back to scan trace history.",
				},
			},
			Required: []string{"correlation_token", "lookback_minutes"},
		},
	}
}

func dynatraceQueryConsumerHealthTool() ToolDefinition {
	return ToolDefinition{
		Name:        "dynatrace_query_consumer_health",
		Description: "Query downstream consumer health (error rate, p95 latency, per consumer) for a sync window. Backed by Dynatrace bizevents and a DQL summarization.",
		Parameters: Schema{
			Type: "OBJECT",
			Properties: map[string]Schema{
				"sync_start_ms": {
					Type:        "INTEGER",
					Description: "Window start as Unix epoch milliseconds.",
				},
				"sync_end_ms": {
					Type:        "INTEGER",
					Description: "Window end as Unix epoch milliseconds.",
				},
			},
			Required: []string{"sync_start_ms", "sync_end_ms"},
		},
	}
}
