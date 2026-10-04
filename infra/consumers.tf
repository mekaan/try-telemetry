# Every file in consumers/ becomes one module instance. A team registering to
# read telemetry writes YAML, not Terraform, and never waits for the cloud
# team to write it for them. The plan is posted on their PR by CI.

locals {
  consumers = {
    for f in fileset("${path.module}/../consumers", "*.yaml") :
    trimsuffix(f, ".yaml") => yamldecode(file("${path.module}/../consumers/${f}"))
  }
}

module "consumer" {
  source   = "./modules/telemetry-consumer"
  for_each = local.consumers

  name              = each.key
  registration      = each.value
  oidc_provider_arn = var.eks_oidc_provider_arn
  msk_cluster_arn   = var.msk_cluster_arn
  topic             = "telemetry.v1"

  permissions_boundary_arn = var.consumer_permissions_boundary_arn
}

# The read API loads this at startup: which service account is which client,
# and its rate limit. Callers authenticate with their projected service
# account token, so API access needs no shared secret.
resource "kubernetes_config_map_v1" "api_clients" {
  metadata {
    name      = "telemetry-api-clients"
    namespace = "telemetry"
  }
  data = {
    "clients.json" = jsonencode({
      for name, m in module.consumer : m.service_account => m.api_client if m.api_client != null
    })
  }
}

variable "eks_oidc_provider_arn" { type = string }
variable "msk_cluster_arn" { type = string }
variable "consumer_permissions_boundary_arn" { type = string }

output "consumer_roles" {
  description = "IAM role per stream consumer; the team annotates its service account with it."
  value       = { for name, m in module.consumer : name => m.role_arn if m.role_arn != null }
}
