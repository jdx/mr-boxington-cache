# Azure production deployment

This deployment runs Caddy, mbx-cache, PostgreSQL, Prometheus, and Grafana on
one Ubuntu VM in Azure East US. Cache objects live in an Azure Blob Storage
container in the same region. The VM's system-assigned managed identity has
`Storage Blob Data Contributor` on that account, so the service and GitHub
Actions hold no storage key.

All new resources use the `mbx-cache` name. The public endpoint is
`https://cache.jdx.dev`; the retired OVH host, R2 bucket, and
`cache.mise.jdx.dev` endpoint are not reused.

## Prerequisites

- an Azure subscription and an account allowed to create resource groups,
  virtual machines, role assignments, and storage accounts;
- Azure CLI, Terraform 1.8 or newer, mise 2026.8.2 or newer, `curl`, `jq`,
  `ssh`, and `tailscale`;
- a remote encrypted Terraform backend for production state;
- an SSH public key; and
- a published mbx-cache image pinned by digest.

The Terraform creates billable Azure resources. The default `Standard_B2s` VM
keeps the current single-host operating model, and ZRS Blob Storage preserves
objects across an availability-zone failure. Review current Azure pricing and
SKU availability in `eastus` before applying.

## Provision Azure

Authenticate, copy the example variables, and choose a globally unique storage
account name:

```sh
az login
cd deploy/azure/terraform
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

Keep production state in a remote encrypted backend. Local state and
`terraform.tfvars` are ignored by Git.

For initial setup only, set `admin_source_cidr` to the operator's public `/32`.
Terraform otherwise exposes only HTTP and HTTPS. The host firewall independently
allows SSH from that exact source and through Tailscale.

## Bootstrap the VM

Use the temporary public SSH rule to install Tailscale, then enroll the host as
`mbx-cache-prod` and verify that its MagicDNS name resolves:

```sh
ssh azureuser@"$(terraform -chdir=deploy/azure/terraform output -raw server_ipv4)"
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --hostname=mbx-cache-prod
```

The exact enrollment flags depend on the tailnet policy; do not put an auth key
in Terraform state or Git.

Export the Terraform outputs and deploy the immutable image:

```sh
export AZURE_STORAGE_ACCOUNT="$(terraform -chdir=deploy/azure/terraform output -raw azure_storage_account)"
export AZURE_STORAGE_CONTAINER="$(terraform -chdir=deploy/azure/terraform output -raw azure_storage_container)"
export AZURE_SSH_HOST="mbx-cache-prod.example-tailnet.ts.net"
export AZURE_SSH_SOURCE_CIDR="$(tailscale ip -4)/32"
export MBX_CACHE_DATABASE_PASSWORD="<stable-password-from-your-password-manager>"
export MBX_CACHE_IMAGE="ghcr.io/jdx/mbx-cache@sha256:<64-hex-digit-digest>"
./deploy/azure/deploy.sh
```

`deploy.sh` builds the GitHub OIDC authorization policy from
`trusted-repositories.json`, writes secrets only into a protected temporary
bootstrap project, and runs `mise bootstrap remote`. It rejects mutable image
tags and waits for `https://cache.jdx.dev/v1/status` after deployment.

Generate the database password once, store it in a password manager, and reuse
it for every deployment. Changing the environment value after PostgreSQL has
initialized does not rotate the database role's password.

After Tailscale works, remove `admin_source_cidr` from `terraform.tfvars` and
apply again. This closes public SSH at the Azure network boundary. Automated
deployments use `tag:mbx-cache-ci` and the VM's `mbx-cache-prod` MagicDNS name;
both must exist in the tailnet policy before the workflow is switched over.

## Cut over production

1. Create a DNS-only A record for `cache.jdx.dev` pointing to the Terraform
   `server_ipv4` output. Keep the old endpoint live during validation.
2. Store `MBX_CACHE_DATABASE_PASSWORD` in the protected `production` GitHub
   Environment. Set `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_CONTAINER`, and
   `AZURE_SSH_HOST` as environment variables.
3. Merge the deployment change and confirm its write/read qualification passes.
4. Update client workflows from `https://cache.mise.jdx.dev` to
   `https://cache.jdx.dev`.
5. Retire the old OVH VM and R2 bucket only after the new cache has warmed and
   the old endpoint no longer receives traffic.

Cache contents are disposable, so the migration intentionally starts with an
empty Blob container instead of copying stale R2 objects. PostgreSQL metadata
also starts empty so it cannot point at blobs that were not migrated.

## Operate and retain data

```sh
curl --fail https://cache.jdx.dev/v1/status
ssh azureuser@"$AZURE_SSH_HOST" \
  'cd /opt/mbx-cache && sudo docker compose ps'
```

Azure deletes cache blobs 30 days after creation and staged upload blobs after
one day through the storage management policy. Periodically sweep metadata at a
slightly longer age:

```sh
sudo docker compose --project-directory /opt/mbx-cache \
  exec cache mbx-cache --sweep-metadata-older-than-days 35
```

Prometheus and Grafana bind only to loopback. Reach Grafana through the
tailnet:

```sh
ssh -N -L 3000:127.0.0.1:3000 azureuser@"$AZURE_SSH_HOST"
```

Then open `http://127.0.0.1:3000/d/mbx-cache/mbx-cache`.
