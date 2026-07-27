variable "region" {
  description = "AWS region. Check c7gn availability if you keep the default agent type."
  type        = string
  default     = "eu-west-2"
}

variable "admin_cidr" {
  description = "CIDR allowed to reach the controller UI/API (your IP as /32). e.g. $(curl -s https://checkip.amazonaws.com)/32"
  type        = string
}

variable "loadr_version" {
  description = "loadr release version to install (e.g. 1.4.2). Empty = latest GitHub release."
  type        = string
  default     = ""
}

variable "github_repo" {
  description = "GitHub repo the release binaries are downloaded from."
  type        = string
  default     = "levantar-ai/loadr"
}

# --- fleet sizing -----------------------------------------------------------
# Start with agent_count = 1 for the calibration phase, then re-apply with the
# full fleet. See the runbook in ../README.md.

variable "agent_count" {
  description = "Number of load-generator agents."
  type        = number
  default     = 16
}

variable "agent_instance_type" {
  description = "Agent instance type. Network-optimized recommended (PPS allowance, not bandwidth, is the ceiling)."
  type        = string
  default     = "c7gn.4xlarge" # 16 vCPU Graviton3E, 50 Gbps. Fallbacks: c6in.4xlarge (x86), c7g.4xlarge
}

variable "target_count" {
  description = "Number of demo-target instances. 2M rps / 12 targets ≈ 167k rps each."
  type        = number
  default     = 12
}

variable "target_instance_type" {
  description = "Target instance type."
  type        = string
  default     = "c7g.4xlarge" # 16 vCPU Graviton3
}

variable "controller_instance_type" {
  description = "Controller instance type (aggregation, HDR merge, web UI)."
  type        = string
  default     = "c7g.2xlarge"
}

variable "enable_placement_group" {
  description = "Put the whole fleet in a cluster placement group (lowest, flattest latency). Disable if spot capacity in one PG is scarce."
  type        = bool
  default     = true
}
