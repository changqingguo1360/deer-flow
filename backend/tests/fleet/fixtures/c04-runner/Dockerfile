FROM deerflow-c04-dependencies:local
COPY *.whl /tmp/wheels/
RUN python -m pip install --no-deps /tmp/wheels/*.whl && rm -rf /tmp/wheels
COPY libexec_bootstrap.py model-bindings.json runtime-bundle.json /opt/deerflow/
COPY c04_worker_fixture.py c04_mcp_fixture.py c04_tool_probe.py /usr/local/lib/python3.12/site-packages/fleet/
RUN touch /usr/local/lib/python3.12/site-packages/fleet/__init__.py
COPY skills/ /opt/deerflow/skills/
USER 65534:65534
