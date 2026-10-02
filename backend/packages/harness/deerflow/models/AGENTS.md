### Model Factory (`packages/harness/deerflow/models/factory.py`)

- `create_chat_model(name, thinking_enabled)` instantiates LLM from config via reflection
- Supports `thinking_enabled` flag with per-model `when_thinking_enabled` overrides
- Supports vLLM-style thinking toggles via `when_thinking_enabled.extra_body.chat_template_kwargs.enable_thinking` for Qwen reasoning models, while normalizing legacy `thinking` configs for backward compatibility
- Supports `supports_vision` flag for image understanding models
- Config values starting with `$` resolved as environment variables
- Missing provider modules surface actionable install hints from reflection resolvers (for example `uv add langchain-google-genai`)

### vLLM Provider (`packages/harness/deerflow/models/vllm_provider.py`)

- `VllmChatModel` subclasses `langchain_openai:ChatOpenAI` for vLLM 0.19.0 OpenAI-compatible endpoints
- Preserves vLLM's non-standard assistant `reasoning` field on full responses, streaming deltas, and follow-up tool-call turns
- Designed for configs that enable thinking through `extra_body.chat_template_kwargs.enable_thinking` on vLLM 0.19.0 Qwen reasoning models, while accepting the older `thinking` alias
- `cumulative_stream_usage` is an opt-in model setting (default `false`) for endpoints that repeat cumulative token totals on each streaming chunk. The provider converts snapshots to deltas only when a stable completion id is present, isolates interleaved streams by id, and leaves the original usage untouched otherwise. Per-model tracking is lock-protected and cleared on the trailing empty-`choices` frame whether or not that frame carries usage. A soft cap of 1024 ids evicts only entries idle for at least one hour; active streams may temporarily exceed the cap so eviction cannot corrupt their deltas. Regression coverage lives in `tests/test_vllm_provider.py`.


### Trusted execution credentials (C04 locally verified)

`models/credentials.py::model_credential_scope(resolver)` binds a private
ContextVar resolver by model name and provider class. The real model factory
injects only its explicit authentication field allowlist at construction;
credentials do not belong in the graph-visible AppConfig or generic caller kwargs.
Scope exit resets the resolver, including exceptions. Async tasks and trusted
thread dispatch must preserve the execution context. Without a scope, Local model
construction keeps its existing configuration behavior.

The host Fleet runner validates the installed model binding's provider, target
and operator version before execution; thinking settings cannot change that target.
That version denotes the approved binding, not immutable vendor weights.
Remote activation remains closed while later ownership fences are under
development. See [Fleet contracts](../../../../../openspec/ecs-fleet-contracts.md).
