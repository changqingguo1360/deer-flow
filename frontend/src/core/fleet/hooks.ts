import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useRef } from "react";
import { toast } from "sonner";

import { UnauthorizedError } from "@/core/api/errors";
import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";

import {
  FleetGoalConflictError,
  fetchFleetTasks,
  operateFleetGoal,
} from "./api";
import { isUnsettledFleetTask } from "./presentation";
import type { FleetGoalAction, FleetGoalOperation } from "./types";

export const fleetTasksQueryKey = (
  owner: string | undefined,
  threadId: string,
) => ["fleet-agent-tasks", owner, threadId] as const;

export function useFleetTasks(threadId: string) {
  const { user } = useAuth();
  return useQuery({
    queryKey: fleetTasksQueryKey(user?.id, threadId),
    queryFn: () => fetchFleetTasks(threadId),
    enabled: Boolean(user?.id && threadId),
    refetchInterval: (query) =>
      query.state.data?.some(isUnsettledFleetTask) ? 3000 : 15000,
    refetchIntervalInBackground: false,
  });
}

export function useFleetGoalOperation(threadId: string) {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const { t } = useI18n();
  const inFlight = useRef(false);
  const mutation = useMutation({
    mutationFn: ({
      action,
      ...operation
    }: FleetGoalOperation & { action: FleetGoalAction }) =>
      operateFleetGoal(threadId, action, operation),
    retry: false,
    onMutate: () => ({ owner: user?.id, threadId }),
    onSuccess: async (_data, _variables, identity) => {
      await queryClient.invalidateQueries({
        queryKey: fleetTasksQueryKey(identity.owner, identity.threadId),
      });
      await Promise.all([
        queryClient.invalidateQueries({
          queryKey: ["thread", identity.threadId],
        }),
        queryClient.invalidateQueries({
          queryKey: ["thread-messages", identity.threadId],
        }),
        queryClient.invalidateQueries({
          queryKey: ["thread-token-usage", identity.threadId],
        }),
      ]);
    },
    onError: async (error: Error, _variables, identity) => {
      if (error instanceof UnauthorizedError) return;
      if (error instanceof FleetGoalConflictError) {
        await queryClient.invalidateQueries({
          queryKey: fleetTasksQueryKey(
            identity?.owner,
            identity?.threadId ?? threadId,
          ),
        });
        toast.error(t.fleetTasks.operationConflict);
      } else {
        toast.error(`${t.fleetTasks.operationFailed}: ${error.message}`);
      }
    },
    onSettled: () => {
      inFlight.current = false;
    },
  });
  return {
    ...mutation,
    mutate: (operation: FleetGoalOperation & { action: FleetGoalAction }) => {
      if (inFlight.current) return;
      inFlight.current = true;
      mutation.mutate(operation);
    },
  };
}
