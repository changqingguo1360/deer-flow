"""Call-time guards on original SDK clients, after serialization before HTTP.

Only the explicitly approved text/tool chat-completions framing contract is
supported. Bytes are a conservative upper bound for byte-BPE text tokens; the
approved per-message framing allowance additionally covers the provider's
control tokens. The contract is operator-approved for its exact model/version,
not inferred from context_window or arbitrary OpenAI-compatible endpoints.
"""

import inspect
import json
from contextvars import ContextVar
from hashlib import sha256

from .call_budget import current_call_budget, mark_private_call_denial

_request_owner = ContextVar("private_budgeted_sdk_request", default=None)
_ALLOWED_FIELDS = frozenset(
    {
        "model",
        "messages",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
        "temperature",
        "top_p",
        "stop",
        "seed",
        "frequency_penalty",
        "presence_penalty",
        "logit_bias",
        "logprobs",
        "top_logprobs",
        "response_format",
        "max_completion_tokens",
        "n",
        "stream",
        "stream_options",
        "reasoning_effort",
    }
)


def validate_contract(contract, *, provider_use, target_model, version):
    keys = {"adapter", "tokenizer", "serialization_revision", "provider_use", "target_model", "version", "max_completion_tokens", "framing_tokens_per_message"}
    if (
        not isinstance(contract, dict)
        or set(contract) != keys
        or provider_use != "langchain_openai:ChatOpenAI"
        or (contract["provider_use"], contract["target_model"], contract["version"]) != (provider_use, target_model, version)
        or contract["adapter"] != "openai-chat-v1"
        or contract["tokenizer"] != "cl100k_base"
        or contract["serialization_revision"] != "utf8-json-framing-v1"
    ):
        raise ValueError("Approved model has no certified budget request adapter")
    if type(contract["max_completion_tokens"]) is not int or not 0 < contract["max_completion_tokens"] <= 2**31 - 1 or type(contract["framing_tokens_per_message"]) is not int or not 32 <= contract["framing_tokens_per_message"] <= 4096:
        raise ValueError("Approved model budget contract has invalid bounds")


def _bound(request, contract, *, model_name, provider_use):
    if (
        contract.get("adapter") != "openai-chat-v1"
        or contract.get("tokenizer") != "cl100k_base"
        or contract.get("serialization_revision") != "utf8-json-framing-v1"
        or contract.get("provider_use") != provider_use
        or not contract.get("version")
    ):
        raise ValueError("unsupported_provider_budget_contract")
    if request.headers.get("x-stainless-raw-response") == "stream":
        raise ValueError("unsupported_unparsed_http_stream")
    if request.method != "POST" or not request.url.path.endswith("/chat/completions") or request.headers.get("content-type", "").split(";", 1)[0] != "application/json":
        raise ValueError("unsupported_provider_request_method")
    raw = request.content
    data = json.loads(raw)
    if not isinstance(data, dict) or set(data) - _ALLOWED_FIELDS or data.get("model") != contract.get("target_model") or (type(data.get("n", 1)) is not int or data.get("n", 1) != 1):
        raise ValueError("unsupported_provider_request_shape")
    maximum, framing = contract.get("max_completion_tokens"), contract.get("framing_tokens_per_message")
    if type(maximum) is not int or not 0 < maximum <= 2**31 - 1 or type(framing) is not int or not 32 <= framing <= 4096 or data.get("max_completion_tokens") != maximum:
        raise ValueError("unenforced_provider_output_cap")
    messages = data.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("unsupported_provider_messages")
    for message in messages:
        if (
            not isinstance(message, dict)
            or set(message) - {"role", "content", "name", "tool_call_id", "tool_calls"}
            or message.get("role") not in {"system", "developer", "user", "assistant", "tool"}
            or message.get("content") is not None
            and not isinstance(message["content"], str)
        ):
            raise ValueError("unsupported_provider_media_or_framing")
        if "tool_calls" in message:
            for call in message["tool_calls"]:
                if (
                    not isinstance(call, dict)
                    or set(call) != {"id", "type", "function"}
                    or call["type"] != "function"
                    or set(call["function"]) != {"name", "arguments"}
                    or not all(isinstance(value, str) for value in call["function"].values())
                ):
                    raise ValueError("unsupported_provider_tool_framing")
    tools = data.get("tools", [])
    if not isinstance(tools, list):
        raise ValueError("unsupported_provider_tools")
    for tool in tools:
        if not isinstance(tool, dict) or set(tool) != {"type", "function"} or tool["type"] != "function" or not isinstance(tool["function"], dict) or set(tool["function"]) - {"name", "description", "parameters", "strict"}:
            raise ValueError("unsupported_provider_tool_framing")
    # Include EVERY wire field's UTF-8 bytes, even fields the provider does not
    # tokenize, rather than reconstructing a possibly incomplete prompt.
    return {"provider_contract": dict(contract) | {"model_name": model_name}, "request_digest": sha256(raw).hexdigest(), "input_bound": len(raw) + framing * (len(messages) + len(tools) + 1), "output_bound": maximum}


def _usage(value):
    usage = getattr(value, "usage", None)
    if usage is None:
        return None
    fields = {"input_tokens": getattr(usage, "prompt_tokens", None), "output_tokens": getattr(usage, "completion_tokens", None), "total_tokens": getattr(usage, "total_tokens", None)}
    if any(type(number) is not int or number < 0 for number in fields.values()) or fields["total_tokens"] != fields["input_tokens"] + fields["output_tokens"]:
        return None
    return fields


class _SyncStream:
    def __init__(self, stream, capability, ticket):
        self.stream, self.capability, self.ticket = stream, capability, ticket
        self.usage = None
        self.finished = self.closed = self.invalid = False

    def __getattr__(self, name):
        return getattr(self.stream, name)

    def __iter__(self):
        try:
            for chunk in self.stream:
                self.observe(chunk)
                yield chunk
            self.finished = True
            self.close()
        except BaseException:
            self.capability.mark_unknown_sync(self.ticket, "stream_error_or_cancel")
            raise
        finally:
            if not self.closed:
                self.close()

    def observe(self, chunk):
        usage = _usage(chunk)
        if getattr(chunk, "usage", None) is not None:
            if usage is None or self.usage is not None and self.usage != usage:
                self.invalid = True
            self.usage = usage

    def close(self):
        if self.closed:
            return
        try:
            self.stream.close()
            if self.finished and self.usage is not None and not self.invalid:
                self.capability.settle_sync(self.ticket, self.usage)
            else:
                self.capability.mark_unknown_sync(self.ticket, "stream_usage_or_close_unknown")
        except BaseException:
            self.capability.mark_unknown_sync(self.ticket, "stream_close_or_authority_lost")
            raise
        finally:
            self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _AsyncStream(_SyncStream):
    async def __aiter__(self):
        try:
            async for chunk in self.stream:
                self.observe(chunk)
                yield chunk
            self.finished = True
            await self.close()
        except BaseException:
            await self.capability.mark_unknown(self.ticket, "stream_error_or_cancel")
            raise
        finally:
            if not self.closed:
                await self.close()

    async def close(self):
        if self.closed:
            return
        try:
            await self.stream.close()
            if self.finished and self.usage is not None and not self.invalid:
                await self.capability.settle_async(self.ticket, self.usage)
            else:
                await self.capability.mark_unknown(self.ticket, "stream_usage_or_close_unknown")
        except BaseException:
            await self.capability.mark_unknown(self.ticket, "stream_close_or_authority_lost")
            raise
        finally:
            self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()


def _guard_client(client, *, model_name, provider_use, asynchronous):
    if getattr(client, "_deerflow_budget_guard", None):
        if client._deerflow_budget_guard != (model_name, provider_use):
            raise ValueError("Conflicting original SDK budget binding")
        return client
    if not callable(getattr(client, "request", None)) or not callable(getattr(client, "_send_request", None)):
        raise ValueError("Unsupported SDK request adapter revision")
    original_request, original_send, original_copy = client.request, client._send_request, client.copy
    client._deerflow_budget_guard = (model_name, provider_use)

    def prepare(capability, request):
        owner = _request_owner.get()
        if owner is None or owner["client"] is not client or owner["ticket"] is not None:
            raise ValueError("unsupported_direct_or_repeated_sdk_transport")
        if getattr(client, "_workload_identity_auth", None) is not None:
            raise ValueError("unsupported_sdk_auth_retry")
        contract = capability.contract(model_name, provider_use)
        return owner, _bound(request, contract, model_name=model_name, provider_use=provider_use)

    async def send_async(request, **kwargs):
        capability = current_call_budget()
        if capability is None:
            return await original_send(request, **kwargs)
        try:
            try:
                owner, bound = prepare(capability, request)
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                await capability.deny_async("unsupported_provider_request")
            owner["ticket"] = await capability.reserve_async(bound)
        except BaseException as error:
            # The SDK wraps send-hook exceptions as network failures. Preserve
            # the original local authority/policy error for normal middleware.
            owner = _request_owner.get()
            if owner is not None and owner["client"] is client:
                mark_private_call_denial(error)
                owner["pretransport_error"] = error
            raise
        kwargs["follow_redirects"] = False
        return await original_send(request, **kwargs)

    def send_sync(request, **kwargs):
        capability = current_call_budget()
        if capability is None:
            return original_send(request, **kwargs)
        try:
            try:
                owner, bound = prepare(capability, request)
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                capability.deny_sync("unsupported_provider_request")
            owner["ticket"] = capability.reserve_sync(bound)
        except BaseException as error:
            # The SDK wraps send-hook exceptions as network failures. Preserve
            # the original local authority/policy error for normal middleware.
            owner = _request_owner.get()
            if owner is not None and owner["client"] is client:
                mark_private_call_denial(error)
                owner["pretransport_error"] = error
            raise
        kwargs["follow_redirects"] = False
        return original_send(request, **kwargs)

    async def request_async(cast_to, options, **kwargs):
        capability = current_call_budget()
        if capability is None:
            return await original_request(cast_to, options, **kwargs)
        owner = {"client": client, "ticket": None}
        token = _request_owner.set(owner)
        try:
            options = options.model_copy(update={"max_retries": 0})
            response = await original_request(cast_to, options, **kwargs)
            if owner["ticket"] is None:
                await capability.deny_async("provider_transport_not_observed")
            parsed = response.parse() if callable(getattr(response, "parse", None)) else response
            if inspect.isawaitable(parsed):
                parsed = await parsed
            if kwargs.get("stream"):
                guarded = _AsyncStream(parsed, capability, owner["ticket"])
                if parsed is not response:
                    response.parse = lambda **fields: guarded
                    return response
                return guarded
            usage = _usage(parsed)
            if usage is None:
                await capability.mark_unknown(owner["ticket"], "provider_usage_unknown")
            else:
                await capability.settle_async(owner["ticket"], usage)
            return response
        except BaseException:
            if owner.get("pretransport_error") is not None:
                raise owner["pretransport_error"] from None
            if owner["ticket"] is not None:
                await capability.mark_unknown(owner["ticket"], "provider_error_or_authority_lost")
            raise
        finally:
            _request_owner.reset(token)

    def request_sync(cast_to, options, **kwargs):
        capability = current_call_budget()
        if capability is None:
            return original_request(cast_to, options, **kwargs)
        owner = {"client": client, "ticket": None}
        token = _request_owner.set(owner)
        try:
            options = options.model_copy(update={"max_retries": 0})
            response = original_request(cast_to, options, **kwargs)
            if owner["ticket"] is None:
                capability.deny_sync("provider_transport_not_observed")
            parsed = response.parse() if callable(getattr(response, "parse", None)) else response
            if kwargs.get("stream"):
                guarded = _SyncStream(parsed, capability, owner["ticket"])
                if parsed is not response:
                    response.parse = lambda **fields: guarded
                    return response
                return guarded
            usage = _usage(parsed)
            if usage is None:
                capability.mark_unknown_sync(owner["ticket"], "provider_usage_unknown")
            else:
                capability.settle_sync(owner["ticket"], usage)
            return response
        except BaseException:
            if owner.get("pretransport_error") is not None:
                raise owner["pretransport_error"] from None
            if owner["ticket"] is not None:
                capability.mark_unknown_sync(owner["ticket"], "provider_error_or_authority_lost")
            raise
        finally:
            _request_owner.reset(token)

    def copy_guarded(**kwargs):
        return _guard_client(original_copy(**kwargs), model_name=model_name, provider_use=provider_use, asynchronous=asynchronous)

    client.request = request_async if asynchronous else request_sync
    client._send_request = send_async if asynchronous else send_sync
    client.copy = client.with_options = copy_guarded
    return client


def guard_model(model, *, model_name, provider_use):
    """Keep the exact model, bindings and callbacks; wrap its original clients."""
    from langchain_openai import ChatOpenAI

    if isinstance(model, ChatOpenAI) and provider_use == "langchain_openai:ChatOpenAI":
        _guard_client(model.root_client, model_name=model_name, provider_use=provider_use, asynchronous=False)
        _guard_client(model.root_async_client, model_name=model_name, provider_use=provider_use, asynchronous=True)
        return model
    # Cached unsupported factory models keep Local behavior, but fail visibly
    # on every private call. No provider path silently loses the capability.
    for method in ("_generate", "_agenerate", "_stream", "_astream"):
        original = getattr(model, method)
        if getattr(original, "_deerflow_budget_guard", False):
            continue
        if inspect.isasyncgenfunction(original):

            async def async_stream(*args, _original=original, **kwargs):
                if capability := current_call_budget():
                    await capability.deny_async("unsupported_model_provider")
                async for item in _original(*args, **kwargs):
                    yield item

            wrapper = async_stream
        elif inspect.iscoroutinefunction(original):

            async def async_call(*args, _original=original, **kwargs):
                if capability := current_call_budget():
                    await capability.deny_async("unsupported_model_provider")
                return await _original(*args, **kwargs)

            wrapper = async_call
        else:

            def sync_call(*args, _original=original, **kwargs):
                if capability := current_call_budget():
                    capability.deny_sync("unsupported_model_provider")
                return _original(*args, **kwargs)

            wrapper = sync_call
        wrapper._deerflow_budget_guard = True
        object.__setattr__(model, method, wrapper)
    return model
