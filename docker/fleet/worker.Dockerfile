# Operator supplies a frozen Linux Python >=3.12 base and offline artifacts.
ARG WORKER_BASE
FROM ${WORKER_BASE}
ARG WORKER_BASE
ARG DOCKER_CLI_SHA256
ENV PYTHONPATH=/opt/fleet PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY --from=fleet_artifacts / /build-artifacts/
RUN case "$WORKER_BASE" in *@sha256:*) ;; *) exit 1;; esac \
    && test ${#DOCKER_CLI_SHA256} -eq 64 \
    && cd /build-artifacts \
    && sha256sum -c SHA256SUMS \
    && printf '%s  docker.tgz\n' "$DOCKER_CLI_SHA256" | sha256sum -c - \
    && python -m pip install --no-cache-dir --no-index --find-links=wheelhouse --require-hashes -r requirements.lock \
    && tar -xzf docker.tgz docker/docker \
    && install -m 0755 docker/docker /usr/local/bin/docker \
    && docker --version \
    && python -m pip check \
    && rm -rf /build-artifacts
COPY backend/packages/ecs-fleet/deerflow_ecs_fleet /opt/fleet/deerflow_ecs_fleet
RUN python -c 'from deerflow_ecs_fleet.worker.__main__ import WorkerSettings, NodeDaemon, DockerContainers, NASWorkspace'
WORKDIR /opt/fleet
ENTRYPOINT ["python", "-m", "deerflow_ecs_fleet.worker"]
CMD ["--settings", "/etc/fleet/worker.json"]
