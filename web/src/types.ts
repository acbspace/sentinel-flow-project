// These mirror the JSON the incidents-api returns. They are hand-written rather
// than generated: the API is small and stable, and a codegen step would be more
// machinery than it saves. If the surface grows, generate them from an OpenAPI
// document instead of letting these drift.

export type IncidentStatus = "open" | "acknowledged" | "resolved";

export type Severity = "debug" | "info" | "warn" | "error" | "critical";

export interface Incident {
  id: string;
  fingerprint: string;
  tenant_id: string;
  service_name: string;
  rule_id: string;
  title: string;
  severity: Severity;
  status: IncidentStatus;
  event_count: number;
  first_seen_at: string;
  last_seen_at: string;
  opened_at: string;
  acknowledged_at?: string;
  resolved_at?: string;
  updated_at: string;
  details?: Record<string, unknown>;
}

export interface Notification {
  id: string;
  incident_id: string;
  level: number;
  target: string;
  contact: string;
  channel: string;
  status: string;
  detail?: Record<string, unknown>;
  sent_at: string;
}

export type RemediationStatus =
  | "pending"
  | "approved"
  | "rejected"
  | "timed_out"
  | "succeeded"
  | "failed"
  | "skipped";

export interface RemediationAction {
  id: string;
  incident_id: string;
  runbook_id: string;
  step_index: number;
  step_name: string;
  action_kind: string;
  mode: "auto" | "approval";
  status: RemediationStatus;
  actor?: string;
  detail?: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface IncidentListResponse {
  incidents: Incident[];
  count: number;
}

export interface NotificationListResponse {
  notifications: Notification[];
  count: number;
}

export interface RemediationListResponse {
  actions: RemediationAction[];
  count: number;
}

/** The single error shape every SentinelFlow API returns. */
export interface ApiErrorBody {
  error: string;
  message: string;
}
