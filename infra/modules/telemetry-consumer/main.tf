# The paved road for reading telemetry. Stream consumers get an IAM role bound
# to their Kubernetes service account (IRSA) with read access to the topic and
# consumer groups under their own prefix. API consumers need no AWS access at
# all: they authenticate to the read API with their projected service account
# token, so they only get an entry in the API's client list. Neither ever gets
# database access.

variable "name" {
  description = "The registration file name. It is the consumer's identity: role name and consumer group prefix."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,30}$", var.name)) && !can(regex("^(telemetry|platform|cloud-platform|ingest|admin)(-|$)", var.name))
    error_message = "name must be lowercase with dashes and must not use a reserved platform prefix."
  }
}
variable "topic" { type = string }
variable "oidc_provider_arn" { type = string }
variable "msk_cluster_arn" { type = string }

variable "permissions_boundary_arn" {
  description = "Boundary on every consumer role, so the CI role that creates them can't mint anything broader."
  type        = string
}

variable "registration" {
  description = "The decoded consumers/<name>.yaml, already checked by platformcheck in CI."
  type = object({
    team            = string
    owner           = string
    service_account = string
    access          = string
    event_types     = list(string)
    rate_limit_rps  = optional(number, 20)
  })

  validation {
    condition     = contains(["stream", "api"], var.registration.access)
    error_message = "access must be stream or api."
  }
}

locals {
  sa_namespace = split("/", var.registration.service_account)[0]
  sa_name      = split("/", var.registration.service_account)[1]
  oidc_issuer  = replace(var.oidc_provider_arn, "/^.*oidc-provider\\//", "")

  # MSK IAM resources share the cluster ARN's name/uuid suffix. Groups are
  # "<name>.<anything>"; names cannot contain dots, so one consumer's prefix
  # can never match another's groups.
  topic_arn  = "${replace(var.msk_cluster_arn, ":cluster/", ":topic/")}/${var.topic}"
  groups_arn = "${replace(var.msk_cluster_arn, ":cluster/", ":group/")}/${var.name}.*"

  tags = {
    team       = var.registration.team
    owner      = var.registration.owner
    managed-by = "telemetry-platform"
    registered = "consumers/${var.name}.yaml"
  }
}

data "aws_iam_policy_document" "trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${local.sa_namespace}:${local.sa_name}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

locals {
  stream = var.registration.access == "stream"
}

resource "aws_iam_role" "consumer" {
  count                = local.stream ? 1 : 0
  name                 = "telemetry-consumer-${var.name}"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
  permissions_boundary = var.permissions_boundary_arn
  tags                 = local.tags
}

data "aws_iam_policy_document" "stream" {
  statement {
    actions   = ["kafka-cluster:Connect"]
    resources = [var.msk_cluster_arn]
  }
  statement {
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
    resources = [local.topic_arn]
  }
  statement {
    actions   = ["kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]
    resources = [local.groups_arn]
  }
}

resource "aws_iam_role_policy" "stream" {
  count  = local.stream ? 1 : 0
  name   = "read-${var.topic}"
  role   = aws_iam_role.consumer[0].id
  policy = data.aws_iam_policy_document.stream.json
}

output "role_arn" {
  description = "Stream consumers annotate their service account with this; null for API consumers."
  value       = local.stream ? aws_iam_role.consumer[0].arn : null
}

output "service_account" { value = var.registration.service_account }

output "api_client" {
  value = var.registration.access != "api" ? null : {
    team           = var.registration.team
    rate_limit_rps = var.registration.rate_limit_rps
    event_types    = var.registration.event_types
  }
}
