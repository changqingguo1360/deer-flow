import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";

import { useAuth } from "@/core/auth/AuthProvider";
import { useI18n } from "@/core/i18n/hooks";

import { cancelFleetRun, fetchFleetTasks } from "./api";
import { isUnsettledFleetTask } from "./presentation";

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

export function useCancelFleetRun(threadId: string) {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const { t } = useI18n();
  return useMutation({
    mutationFn: (runId: string) => cancelFleetRun(threadId, runId),
    onSuccess: () => {
      void queryClient.invalidateQueries({
        queryKey: fleetTasksQueryKey(user?.id, threadId),
      });
    },
    onError: (error: Error) => {
      toast.error(`${t.backgroundTasks.cancelFailed}: ${error.message}`);
    },
  });
}
