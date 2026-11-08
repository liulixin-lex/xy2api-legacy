package resilience

import (
	"strings"
	"testing"
)

func TestResponsesUnknownReadyDisposition(t *testing.T) {
	for _, tc := range []struct {
		name, event, payload string
		prelude              bool
	}{
		{"gateway ready", "sub2api.gateway_ready", `{"type":"sub2api.gateway_ready"}`, true},
		{"other namespace", "relay.ready", `{"type":"relay.ready","ready":true,"elapsed":0,"metadata":{"empty":[null,{},""]}}`, true},
		{"future response event", "response.vendor_ready", `{"type":"response.vendor_ready","id":"opaque","sequence_number":2}`, true},
		{"SSE type only", "relay.ready", `{}`, true},
		{"payload type only", "", `{"type":"relay.ready"}`, true},
		{"text", "relay.ready", `{"type":"relay.ready","text":"hello"}`, false},
		{"nested content", "relay.ready", `{"type":"relay.ready","extra":{"x":[{"text":"hello"}]}}`, false},
		{"whitespace is content", "relay.ready", `{"type":"relay.ready","text":" "}`, false},
		{"encrypted content", "relay.ready", `{"type":"relay.ready","encrypted_content":"opaque"}`, false},
		{"tool identity", "relay.ready", `{"type":"relay.ready","item":{"type":"function_call","name":"tool"}}`, false},
		{"malformed", "relay.ready", `{"type":"relay.ready"`, false},
		{"not object", "relay.ready", `[]`, false},
		{"duplicate", "relay.ready", `{"type":"relay.ready","ready":true,"ready":false}`, false},
		{"wrong identity type", "relay.ready", `{"type":"relay.ready","id":true}`, false},
		{"wrong event type", "relay.ready", `{"type":true}`, false},
		{"conflicting types", "relay.ready", `{"type":"other.ready"}`, false},
		{"known declaration", "response.output_text.delta", `{"type":"relay.ready"}`, false},
		{"untyped object", "", `{}`, false},
		{"bounded recursion", "relay.ready", `{"type":"relay.ready","extra":` + strings.Repeat(`[`, responsesEmptyFieldMaxDepth+1) + `null` + strings.Repeat(`]`, responsesEmptyFieldMaxDepth+1) + `}`, false},
		{"known tool progress", "response.web_search_call.in_progress", `{"type":"response.web_search_call.in_progress"}`, false},
		{"known refusal", "response.refusal.done", `{"type":"response.refusal.done","refusal":""}`, false},
		{"known malformed delta", "response.output_text.delta", `{"type":"response.output_text.delta"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ResponsesEventOutputDisposition(tc.event, []byte(tc.payload))
			if !got.Matched || (got.Kind == Prelude) != tc.prelude || got.Visible == tc.prelude || got.SemanticOutput == tc.prelude {
				t.Fatalf("got %+v, want prelude=%t", got, tc.prelude)
			}
			if class := ClassifyStreamEvent("responses", tc.event, []byte(tc.payload)); (class.Kind == Prelude) != tc.prelude {
				t.Fatalf("classification %+v disagrees with disposition %+v", class, got)
			}
		})
	}
}
