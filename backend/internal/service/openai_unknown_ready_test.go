//go:build unit

package service

import (
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/liulixin-lex/xy2api/internal/resilience"
	"github.com/stretchr/testify/require"
)

const unknownReadyFrame = "event: sub2api.gateway_ready\ndata: {\"type\":\"sub2api.gateway_ready\",\"ready\":true}\n\n"

func TestResponsesUnknownReadyStallFailsOverAndSucceeds(t *testing.T) {
	withResilienceSettings(t, nil)
	for _, path := range []string{"native", "passthrough", "chat", "messages"} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, recorder := compatStreamContext("/v1/responses", `{}`)
				state := resilience.NewRequestState()
				c.Request = c.Request.WithContext(resilience.WithRequestState(c.Request.Context(), state))
				InstallDeliveryTracking(c, time.Now())
				svc := compatStreamService(&silentUpstream{headers: true, before: unknownReadyFrame}, 1, 1, 0)
				account := rawChatCompletionsTestAccount()
				var err error
				if path == "native" || path == "passthrough" {
					reader, writer := io.Pipe()
					defer func() { _ = reader.Close(); _ = writer.Close() }()
					go func() { _, _ = io.WriteString(writer, unknownReadyFrame) }()
					_, err = runScalarExtraPlaceholderAttempt(svc, c, account, path, reader)
				} else if path == "chat" {
					_, err = svc.ForwardAsChatCompletions(c.Request.Context(), c, account, []byte(compatChatStreamBody), "", "")
				} else {
					_, err = svc.ForwardAsAnthropic(c.Request.Context(), c, account, []byte(compatMessagesStreamBody), "", "")
				}
				requireFirstOutputFailover(t, err)
				require.Empty(t, recorder.Body.String(), "failed ready event must never reach the client")
				require.Zero(t, state.DeliveredBytes())
				require.True(t, state.FirstOutput().IsZero())
				require.True(t, state.SemanticOutputAt().IsZero())
				require.Empty(t, state.PreludeTrace().ReleaseEvent)

				account = rawChatCompletionsTestAccount()
				account.ID++
				state.SetAttemptProfile(resilience.AttemptProfile{AccountID: account.ID, AttemptStart: time.Now(), Stream: true})
				_, visible, terminal := scalarExtraPlaceholderStream()
				_, err = runScalarExtraPlaceholderAttempt(svc, c, account, path, io.NopCloser(strings.NewReader(keepaliveStallCreated+visible+terminal)))
				require.NoError(t, err)
				require.Contains(t, recorder.Body.String(), "ok")
				require.NotContains(t, recorder.Body.String(), "sub2api.gateway_ready")
				require.False(t, state.SemanticOutputAt().IsZero())
			})
		})
	}
}

func TestResponsesUnknownReadyReleasesWithContentInOrder(t *testing.T) {
	withResilienceSettings(t, nil)
	for _, path := range []string{"native", "passthrough", "chat", "messages"} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, recorder := compatStreamContext("/v1/responses", `{}`)
				state := resilience.NewRequestState()
				c.Request = c.Request.WithContext(resilience.WithRequestState(c.Request.Context(), state))
				InstallDeliveryTracking(c, time.Now())
				svc := compatStreamService(nil, 0, 0, 0)
				reader, writer := io.Pipe()
				defer func() { _ = reader.Close(); _ = writer.Close() }()
				done := make(chan error, 1)
				go func() {
					_, err := runScalarExtraPlaceholderAttempt(svc, c, rawChatCompletionsTestAccount(), path, reader)
					done <- err
				}()
				prelude := unknownReadyFrame + keepaliveStallCreated + "event: relay.ready\ndata: {\"type\":\"relay.ready\",\"sequence_number\":1}\n\n"
				_, err := io.WriteString(writer, prelude)
				require.NoError(t, err)
				synctest.Wait()
				require.Empty(t, recorder.Body.String())
				require.True(t, state.SemanticOutputAt().IsZero())
				_, visible, terminal := scalarExtraPlaceholderStream()
				_, err = io.WriteString(writer, visible)
				require.NoError(t, err)
				synctest.Wait()
				require.Contains(t, recorder.Body.String(), "ok", "release must happen before the terminal")
				if path == "native" || path == "passthrough" {
					require.Equal(t, prelude+visible, recorder.Body.String(), "raw frames preserve bytes and order")
				}
				require.Equal(t, "response.output_text.delta", state.PreludeTrace().ReleaseEvent)
				_, err = io.WriteString(writer, terminal)
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				require.NoError(t, <-done)
				if path == "chat" || path == "messages" {
					control, controlRecorder := compatStreamContext("/v1/responses", `{}`)
					_, err = runScalarExtraPlaceholderAttempt(svc, control, rawChatCompletionsTestAccount(), path, io.NopCloser(strings.NewReader(keepaliveStallCreated+visible+terminal)))
					require.NoError(t, err)
					require.Equal(t, controlRecorder.Body.String(), recorder.Body.String(), "conversion preserves the existing frame sequence")
				}
			})
		})
	}
}
