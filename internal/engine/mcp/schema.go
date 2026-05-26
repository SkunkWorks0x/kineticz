package mcp

// ToolDefinition is the Gemini function-calling tool schema. Field names and
// types match the Vertex AI Generative AI API's FunctionDeclaration shape:
// uppercase OpenAPI-style types (STRING, NUMBER, INTEGER, BOOLEAN, OBJECT,
// ARRAY) and a single Parameters object describing the call payload.
type ToolDefinition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  Schema `json:"parameters"`
}

// Schema describes a single parameter or nested type. Items applies when
// Type=="ARRAY". Properties + Required apply when Type=="OBJECT".
type Schema struct {
	Type        string            `json:"type"`
	Description string            `json:"description,omitempty"`
	Properties  map[string]Schema `json:"properties,omitempty"`
	Items       *Schema           `json:"items,omitempty"`
	Required    []string          `json:"required,omitempty"`
	Enum        []string          `json:"enum,omitempty"`
}
