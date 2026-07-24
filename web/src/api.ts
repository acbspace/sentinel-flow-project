import type {
  ApiErrorBody,
  Incident,
  IncidentListResponse,
  IncidentStatus,
  NotificationListResponse,
  RemediationListResponse,
} from "./types";

/**
 * ApiError carries the server's own error code and message.
 *
 * The API distinguishes 404 (no such incident), 409 (illegal transition, or
 * nothing awaiting a decision) and 503 (remediation not configured), and each
 * means something different to a user. Collapsing them into "request failed"
 * would throw away the part that tells them what to do next.
 */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: { Accept: "application/json", ...(init?.headers ?? {}) },
  });

  if (!response.ok) {
    // Every endpoint returns the same error shape, but a proxy or a crash could
    // still produce something else; fall back rather than throwing while
    // throwing.
    let code = "request_failed";
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as ApiErrorBody;
      if (body?.error) code = body.error;
      if (body?.message) message = body.message;
    } catch {
      // Keep the status-line fallback.
    }
    throw new ApiError(response.status, code, message);
  }

  // 202 responses carry a body; 204 would not. Guard for both.
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export function listIncidents(status?: IncidentStatus | ""): Promise<IncidentListResponse> {
  const params = new URLSearchParams({ limit: "100" });
  if (status) params.set("status", status);
  return request<IncidentListResponse>(`/v1/incidents?${params.toString()}`);
}

export function getIncident(id: string): Promise<Incident> {
  return request<Incident>(`/v1/incidents/${id}`);
}

export function listNotifications(id: string): Promise<NotificationListResponse> {
  return request<NotificationListResponse>(`/v1/incidents/${id}/notifications`);
}

export function listRemediation(id: string): Promise<RemediationListResponse> {
  return request<RemediationListResponse>(`/v1/incidents/${id}/remediation`);
}

export function acknowledgeIncident(id: string): Promise<Incident> {
  return request<Incident>(`/v1/incidents/${id}/acknowledge`, { method: "POST" });
}

export function resolveIncident(id: string): Promise<Incident> {
  return request<Incident>(`/v1/incidents/${id}/resolve`, { method: "POST" });
}

export function decideRemediation(
  id: string,
  decision: "approve" | "reject",
  actor: string,
): Promise<unknown> {
  const params = new URLSearchParams({ actor });
  return request<unknown>(`/v1/incidents/${id}/remediation/${decision}?${params.toString()}`, {
    method: "POST",
  });
}
