# Operator-provided Linux Python >=3.12 base and offline, nonsecret artifacts.
# agent_artifacts is a named BuildKit context: SHA256SUMS, requirements.lock,
# wheelhouse (app, harness, extension-api, Fleet and approved provider deps),
# approved model-bindings/runtime-bundle/workspace-contracts JSON and skills/.
ARG AGENT_BASE
FROM ${AGENT_BASE}
ARG AGENT_BASE
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY --from=agent_artifacts / /build-artifacts/
RUN case "$AGENT_BASE" in *@sha256:*) ;; *) exit 1;; esac \
    && cd /build-artifacts \
    && sha256sum -c SHA256SUMS \
    && python -m pip install --no-cache-dir --no-index --find-links=wheelhouse --require-hashes -r requirements.lock \
    && python -m pip check \
    && python -c 'from importlib.metadata import distribution, entry_points; [distribution(name) for name in ("deer-flow", "deerflow-harness", "deerflow-extension-api", "deerflow-ecs-fleet")]; assert len(tuple(entry_points(group="deerflow.fleet.agent_environment", name="gateway"))) == 1' \
    && mkdir -p /opt/deerflow /workspace \
    && python -c 'from pathlib import Path; from deerflow_ecs_fleet.worker import libexec_bootstrap, workspace_collector; import shutil; shutil.copyfile(libexec_bootstrap.__file__, "/opt/deerflow/libexec_bootstrap.py"); shutil.copyfile(workspace_collector.__file__, workspace_collector.COLLECTOR_PATH)' \
    && cp model-bindings.json runtime-bundle.json workspace-contracts.json /opt/deerflow/ \
    && cp -R skills /opt/deerflow/skills \
    && chmod -R a-w /opt/deerflow \
    && chown 65534:65534 /workspace \
    && rm -rf /build-artifacts
USER 65534:65534
WORKDIR /workspace
ENTRYPOINT ["python"]
CMD ["-I", "-S", "/opt/deerflow/libexec_bootstrap.py", "--provider", "gateway"]
