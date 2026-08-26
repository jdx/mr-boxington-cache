#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
terraform_dir="$script_dir/terraform"
terraform_command="${TERRAFORM_COMMAND:-terraform}"

for command in curl jq mise "$terraform_command"; do
  if ! command -v "$command" >/dev/null; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

if command -v sha256sum >/dev/null; then
  sha256_command=(sha256sum)
elif command -v shasum >/dev/null; then
  sha256_command=(shasum -a 256)
else
  echo "required command not found: sha256sum or shasum" >&2
  exit 1
fi

if ! mise bootstrap remote --help >/dev/null 2>&1; then
  echo "mise 2026.8.2 or newer with 'bootstrap remote' is required" >&2
  exit 1
fi

require_env() {
  local name=$1
  if [[ -z ${!name-} ]]; then
    echo "$name must be set" >&2
    exit 1
  fi
}

for name in \
  MBX_CACHE_DATABASE_PASSWORD \
  MBX_CACHE_IMAGE \
  OVH_SSH_SOURCE_CIDR \
  R2_ACCESS_KEY_ID \
  R2_SECRET_ACCESS_KEY; do
  require_env "$name"
done

if [[ ! $MBX_CACHE_DATABASE_PASSWORD =~ ^[A-Za-z0-9_-]{24,}$ ]]; then
  echo "MBX_CACHE_DATABASE_PASSWORD must contain at least 24 URL-safe characters" >&2
  exit 1
fi
if [[ ! $MBX_CACHE_IMAGE =~ ^.+@sha256:[a-fA-F0-9]{64}$ ]]; then
  echo "MBX_CACHE_IMAGE must be pinned by sha256 digest" >&2
  exit 1
fi
if [[ $OVH_SSH_SOURCE_CIDR =~ [[:space:]] ]]; then
  echo "OVH_SSH_SOURCE_CIDR must be one CIDR without whitespace" >&2
  exit 1
fi

trusted_repositories_file=${MBX_CACHE_GITHUB_REPOSITORIES_FILE:-$script_dir/trusted-repositories.json}
deployment_repository=${MBX_CACHE_DEPLOY_GITHUB_REPOSITORY:-jdx/mr-boxington-cache}
deployment_owner_id=${MBX_CACHE_DEPLOY_GITHUB_OWNER_ID:-216188}
deployment_workflow_ref=${MBX_CACHE_DEPLOY_GITHUB_WORKFLOW_REF:-jdx/mr-boxington-cache/.github/workflows/release-plz.yml@refs/heads/main}
ssh_user=${OVH_SSH_USER:-ubuntu}
ssh_port=${OVH_SSH_PORT:-22}

if [[ ! -r $trusted_repositories_file ]]; then
  echo "MBX_CACHE_GITHUB_REPOSITORIES_FILE is not readable: $trusted_repositories_file" >&2
  exit 1
fi
if ! trusted_repositories=$(jq -ce '
  if type != "array" or length == 0 then
    error("must be a non-empty array")
  elif any(.[].repository; type != "string" or test("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$") | not) then
    error("repository must be an owner/repository name")
  elif any(.[].repository_owner_id; type != "string" or test("^[0-9]+$") | not) then
    error("repository_owner_id must be a numeric string")
  elif ([.[].repository] | unique | length) != length then
    error("repository names must be unique")
  else
    map({repository, repository_owner_id})
  end
' "$trusted_repositories_file"); then
  echo "MBX_CACHE_GITHUB_REPOSITORIES_FILE is invalid: $trusted_repositories_file" >&2
  exit 1
fi
if [[ ! $deployment_repository =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
  echo "MBX_CACHE_DEPLOY_GITHUB_REPOSITORY must be an owner/repository name" >&2
  exit 1
fi
if [[ ! $deployment_owner_id =~ ^[0-9]+$ ]]; then
  echo "MBX_CACHE_DEPLOY_GITHUB_OWNER_ID must be numeric" >&2
  exit 1
fi
if [[ ! $deployment_workflow_ref =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[.]github/workflows/[A-Za-z0-9_.-]+[.]ya?ml@refs/heads/[A-Za-z0-9_./-]+$ ]]; then
  echo "MBX_CACHE_DEPLOY_GITHUB_WORKFLOW_REF must identify a workflow on a branch" >&2
  exit 1
fi
if [[ ! $ssh_user =~ ^[A-Za-z_][A-Za-z0-9_-]*$ ]]; then
  echo "OVH_SSH_USER is not a valid SSH user name" >&2
  exit 1
fi
if [[ ! $ssh_port =~ ^[0-9]+$ ]] || ((ssh_port < 1 || ssh_port > 65535)); then
  echo "OVH_SSH_PORT must be between 1 and 65535" >&2
  exit 1
fi
if [[ -n ${OVH_SSH_IDENTITY_FILE:-} && ! -r $OVH_SSH_IDENTITY_FILE ]]; then
  echo "OVH_SSH_IDENTITY_FILE is not readable: $OVH_SSH_IDENTITY_FILE" >&2
  exit 1
fi

server_ip="$("$terraform_command" -chdir="$terraform_dir" output -raw server_ipv4)"
ssh_host=${OVH_SSH_HOST:-$server_ip}
cache_url="$("$terraform_command" -chdir="$terraform_dir" output -raw cache_url)"
r2_bucket="$("$terraform_command" -chdir="$terraform_dir" output -raw r2_bucket)"
r2_endpoint="$("$terraform_command" -chdir="$terraform_dir" output -raw r2_endpoint)"
cache_url=${cache_url%/}

if [[ ! $ssh_host =~ ^[A-Za-z0-9_.:-]+$ ]]; then
  echo "OVH_SSH_HOST must be an IP address or SSH host name without whitespace" >&2
  exit 1
fi

if [[ $cache_url != https://* ]]; then
  echo "Terraform cache_url must use https://" >&2
  exit 1
fi
cache_domain=${cache_url#https://}
if [[ ! $cache_domain =~ ^[A-Za-z0-9.-]+$ ]]; then
  echo "Terraform cache_url must contain a DNS hostname without a path" >&2
  exit 1
fi
if [[ ! $r2_endpoint =~ ^https://[a-f0-9]+[.]r2[.]cloudflarestorage[.]com$ ]]; then
  echo "Terraform r2_endpoint is not a Cloudflare R2 endpoint" >&2
  exit 1
fi

oidc_providers=$(jq -cn \
  --arg audience "$cache_url" \
  --arg deployment_repository "$deployment_repository" \
  --arg deployment_owner_id "$deployment_owner_id" \
  --arg deployment_workflow_ref "$deployment_workflow_ref" \
  --argjson repositories "$trusted_repositories" \
  '[{
    issuer: "https://token.actions.githubusercontent.com",
    audiences: [$audience],
    rules: (
      (
        [$repositories[] as $entry | [
          {claims: {repository: $entry.repository, repository_owner_id: $entry.repository_owner_id, ref: "refs/heads/main"}, read: [$entry.repository], write: [$entry.repository]},
          {claims: {repository: $entry.repository, repository_owner_id: $entry.repository_owner_id, ref_type: "tag"}, read: [$entry.repository], write: []},
          {claims: {repository: $entry.repository, repository_owner_id: $entry.repository_owner_id, event_name: "pull_request"}, read: [$entry.repository], write: []},
          {claims: {repository: $entry.repository, repository_owner_id: $entry.repository_owner_id, event_name: "push"}, read: [$entry.repository], write: []}
        ]] | add
      ) + [
        {claims: {repository: $deployment_repository, repository_owner_id: $deployment_owner_id, environment: "production", workflow_ref: $deployment_workflow_ref}, read: [$deployment_repository], write: [$deployment_repository]}
      ]
    )
  }]')

temporary_root=${TMPDIR:-/tmp}
temporary_root=${temporary_root%/}
project_dir=$(mktemp -d "$temporary_root/mbx-cache-ovh.XXXXXXXX")

cleanup() {
  local status=$?
  case "$project_dir" in
    "$temporary_root"/mbx-cache-ovh.*) rm -rf -- "$project_dir" ;;
    *) echo "refusing to remove unexpected temporary directory: $project_dir" >&2 ;;
  esac
  return "$status"
}
trap cleanup EXIT

cp -R "$script_dir/bootstrap/." "$project_dir/"
install -d -m 0700 "$project_dir/runtime"

file_sha256() {
  local digest
  read -r digest _ < <("${sha256_command[@]}" "$1")
  printf '%s\n' "$digest"
}

prometheus_config_hash=$(file_sha256 "$project_dir/monitoring/prometheus.yml")
grafana_config_hash=$({
  file_sha256 "$project_dir/monitoring/grafana/dashboards/mise-cache.json"
  file_sha256 "$project_dir/monitoring/grafana/provisioning/dashboards/mise-cache.yml"
  file_sha256 "$project_dir/monitoring/grafana/provisioning/datasources/prometheus.yml"
} | "${sha256_command[@]}")
grafana_config_hash=${grafana_config_hash%% *}

write_dotenv() {
  local encoded key=$1 value=$2
  encoded=$(jq -Rn --arg value "$value" '$value')
  printf '%s=%s\n' "$key" "$encoded"
}

{
  write_dotenv MBX_CACHE_DOMAIN "$cache_domain"
  write_dotenv MBX_CACHE_GRAFANA_CONFIG_HASH "$grafana_config_hash"
  write_dotenv MBX_CACHE_IMAGE "$MBX_CACHE_IMAGE"
  write_dotenv MBX_CACHE_PROMETHEUS_CONFIG_HASH "$prometheus_config_hash"
  write_dotenv POSTGRES_PASSWORD "$MBX_CACHE_DATABASE_PASSWORD"
} >"$project_dir/runtime/.env"

{
  write_dotenv MBX_CACHE_STORAGE s3
  write_dotenv MBX_CACHE_DATABASE_URL \
    "postgres://mise_cache:$MBX_CACHE_DATABASE_PASSWORD@postgres/mise_cache"
  write_dotenv MBX_CACHE_S3_BUCKET "$r2_bucket"
  write_dotenv MBX_CACHE_S3_PREFIX v1
  write_dotenv MBX_CACHE_S3_ENDPOINT "$r2_endpoint"
  write_dotenv MBX_CACHE_S3_REGION auto
  write_dotenv MBX_CACHE_S3_PATH_STYLE true
  write_dotenv MBX_CACHE_OIDC_PROVIDERS_JSON "$oidc_providers"
  write_dotenv MBX_CACHE_ALLOW_ANONYMOUS false
  write_dotenv AWS_ACCESS_KEY_ID "$R2_ACCESS_KEY_ID"
  write_dotenv AWS_SECRET_ACCESS_KEY "$R2_SECRET_ACCESS_KEY"
} >"$project_dir/runtime/cache.env"
chmod 0600 "$project_dir/runtime/.env" "$project_dir/runtime/cache.env"

ssh_source_cidr=$(jq -Rn --arg value "$OVH_SSH_SOURCE_CIDR" '$value')
{
  printf '%s\n' '[[bootstrap.linux.firewall.rules]]'
  printf '%s\n' 'name = "ssh-admin"'
  printf '%s\n' 'direction = "incoming"'
  printf '%s\n' 'action = "allow"'
  printf 'port = %s\n' "$ssh_port"
  printf '%s\n' 'protocol = "tcp"'
  printf 'source = %s\n' "$ssh_source_cidr"
} >"$project_dir/mise.local.toml"

remote_args=(
  bootstrap remote
  --host "$ssh_user@$ssh_host"
  --source "$project_dir"
  --only "packages,files,services,firewall,compose"
  --update
  --yes
)
if [[ $ssh_port != 22 ]]; then
  remote_args+=(--port "$ssh_port")
fi
if [[ -n ${OVH_SSH_IDENTITY_FILE:-} ]]; then
  remote_args+=(--identity-file "$OVH_SSH_IDENTITY_FILE")
fi

dry_run=false
for argument in "$@"; do
  if [[ $argument == --dry-run || $argument == -n ]]; then
    dry_run=true
  fi
done

mise "${remote_args[@]}" "$@"

if [[ $dry_run == false ]]; then
  curl \
    --connect-timeout 10 \
    --fail \
    --retry 60 \
    --retry-all-errors \
    --retry-delay 5 \
    --retry-max-time 600 \
    --show-error \
    --silent \
    "$cache_url/v1/status" >/dev/null
  echo "mbx-cache is healthy at $cache_url"
fi
