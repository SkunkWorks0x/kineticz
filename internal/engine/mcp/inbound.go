package mcp

// InboundTools returns the MCP tool schemas Kineticz exposes to external
// callers (including Gemini). Currently exposes Diagnose only. Repair is
// pending implementation; once internal/engine/repair lands, add repairTool()
// to the returned slice.
func InboundTools() []ToolDefinition {
	return []ToolDefinition{
		diagnoseTool(),
	}
}

func diagnoseTool() ToolDefinition {
	return ToolDefinition{
		Name:        "kineticz_diagnose",
		Description: "Run a parallel Elastic + Dynatrace diagnosis under a 5-second timeout. Returns the contract context, optional consumer health, and a Degraded flag set when Dynatrace soft-failed (telemetry unavailable, Elastic context still returned).",
		Parameters: Schema{
			Type: "OBJECT",
			Properties: map[string]Schema{
				"contract_name":     {Type: "STRING", Description: "Identifier of the contract."},
				"columns":           {Type: "ARRAY", Items: &Schema{Type: "STRING"}, Description: "Column names from the failing pipeline."},
				"diff_embedding":    {Type: "ARRAY", Items: &Schema{Type: "NUMBER"}, Description: "Diff embedding vector."},
				"sync_start_ms":     {Type: "INTEGER", Description: "Window start (Unix ms)."},
				"sync_end_ms":       {Type: "INTEGER", Description: "Window end (Unix ms)."},
				"correlation_token": {Type: "STRING", Description: "Caller-provided correlation token propagated through the audit chain."},
			},
			Required: []string{"contract_name", "columns", "diff_embedding", "sync_start_ms", "sync_end_ms", "correlation_token"},
		},
	}
}
