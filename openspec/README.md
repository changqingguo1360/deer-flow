# OpenSpec for DeerFlow ECS Fleet

本目录为 2026-10-01 ECS Fleet 规划初始化，使用本机 OpenSpec 1.4.1 的 spec-driven schema。
没有生成/覆写项目 AGENTS.md、CLAUDE.md 或全局 skills；初始化使用 `openspec init --tools none`。

实施顺序：add-ecs-fleet-jobs → add-ecs-remote-agent → add-ecs-agent-job-continuations。
入口：[规划总览](../docs/superpowers/plans/2026-10-01-ecs-fleet-roadmap.md)。

`specs/` 暂无已交付基线，需求在各 change 的 ADDED Requirements 中。实现通过后再归档，不能因为规划校验通过而 archive。
