#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/mbx-cache-deploy-test.XXXXXXXX")

cleanup() {
  local status=$?
  case "$test_root" in
    "${TMPDIR:-/tmp}"/mbx-cache-deploy-test.*) rm -rf -- "$test_root" ;;
    *) echo "refusing to remove unexpected test directory: $test_root" >&2 ;;
  esac
  return "$status"
}
trap cleanup EXIT

install -d "$test_root/bin" "$test_root/capture"

cat >"$test_root/bin/fake-terraform" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "${*: -1}" in
  server_ipv4) printf '%s\n' '192.0.2.10' ;;
  cache_url) printf '%s\n' 'https://cache.example.com' ;;
  r2_bucket) printf '%s\n' 'mbx-cache-production' ;;
  r2_endpoint) printf '%s\n' 'https://0123456789abcdef.r2.cloudflarestorage.com' ;;
  *) echo "unexpected Terraform output: ${*: -1}" >&2; exit 1 ;;
esac
SH

cat >"$test_root/bin/mise" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ $* == 'bootstrap remote --help' ]]; then
  exit 0
fi

printf '%s\n' "$@" >"$CAPTURE_DIR/args"
project_dir=
previous=
for argument in "$@"; do
  if [[ $previous == --source ]]; then
    project_dir=$argument
    break
  fi
  previous=$argument
done
[[ -n $project_dir && -d $project_dir ]]
printf '%s\n' "$project_dir" >"$CAPTURE_DIR/project-dir"
[[ $(stat -c %a "$project_dir/runtime") == 700 ]]
[[ $(stat -c %a "$project_dir/runtime/.env") == 600 ]]
[[ $(stat -c %a "$project_dir/runtime/cache.env") == 600 ]]
grep -Fq 'POSTGRES_PASSWORD="database_password_123456"' "$project_dir/runtime/.env"
prometheus_config_hash=$(sha256sum "$project_dir/monitoring/prometheus.yml")
prometheus_config_hash=${prometheus_config_hash%% *}
grep -Fq "MBX_CACHE_PROMETHEUS_CONFIG_HASH=\"$prometheus_config_hash\"" "$project_dir/runtime/.env"
grafana_config_hash=$({
  sha256sum "$project_dir/monitoring/grafana/dashboards/mise-cache.json"
  sha256sum "$project_dir/monitoring/grafana/provisioning/dashboards/mise-cache.yml"
  sha256sum "$project_dir/monitoring/grafana/provisioning/datasources/prometheus.yml"
} | awk '{print $1}' | sha256sum)
grafana_config_hash=${grafana_config_hash%% *}
grep -Fq "MBX_CACHE_GRAFANA_CONFIG_HASH=\"$grafana_config_hash\"" "$project_dir/runtime/.env"
grep -Fq 'AWS_ACCESS_KEY_ID="r2-access-key"' "$project_dir/runtime/cache.env"
grep -Fq 'AWS_SECRET_ACCESS_KEY="r2-secret-key"' "$project_dir/runtime/cache.env"
oidc_json=$(sed -n 's/^MBX_CACHE_OIDC_PROVIDERS_JSON=//p' "$project_dir/runtime/cache.env" | jq -c fromjson)
jq -e --argjson repositories "${MBX_CACHE_TEST_REPOSITORIES:?}" '
  .[0].rules as $rules |
  # Four rules per trusted repository -- protected-main push, tag, pull
  # request, other push -- plus the single deployment rule. Both the list and
  # the count come from the allowlist the deploy just consumed, so adding a
  # repository does not fail this on a restated name or a stale number. What
  # is still checked is that every configured repository got exactly those
  # four rules and that nothing generated a rule beyond them.
  ($rules | length == ($repositories | length) * 4 + 1) and
  ($repositories | all(. as $repository |
    ($rules | any(
      .claims.repository == $repository and
      .claims.repository_owner_id == "216188" and
      .claims.event_name == "push" and
      .claims.ref_type == "branch" and
      .claims.ref == "refs/heads/main" and
      .read == [$repository] and
      .write == [$repository]
    )) and
    ($rules | any(
      .claims.repository == $repository and
      .claims.repository_owner_id == "216188" and
      .claims.ref_type == "tag" and
      .read == [$repository] and
      .write == []
    )) and
    ($rules | any(
      .claims.repository == $repository and
      .claims.repository_owner_id == "216188" and
      .claims.event_name == "pull_request" and
      .read == [$repository] and
      .write == []
    )) and
    ($rules | any(
      .claims.repository == $repository and
      .claims.repository_owner_id == "216188" and
      .claims.event_name == "push" and
      .read == [$repository] and
      .write == []
    ))
  )) and
  ($rules | any(
    .claims.repository == "jdx/mr-boxington-cache" and
    .claims.repository_owner_id == "216188" and
    .claims.environment == "production" and
    .claims.workflow_ref == "jdx/mr-boxington-cache/.github/workflows/release-plz.yml@refs/heads/main" and
    .read == ["jdx/mr-boxington-cache"] and
    .write == ["jdx/mr-boxington-cache"]
  ))
' <<<"$oidc_json" >/dev/null
grep -Fq 'port = 2222' "$project_dir/mise.local.toml"
grep -Fq 'source = "203.0.113.10/32"' "$project_dir/mise.local.toml"
if grep -R -Fq 'must-not-be-forwarded' "$project_dir"; then
  echo "unrelated caller environment was copied" >&2
  exit 1
fi
SH
chmod 0755 "$test_root/bin/fake-terraform" "$test_root/bin/mise"

# The rule assertions in the fake mise above run in a quoted heredoc, so the
# allowlist has to reach them through the environment rather than expansion.
trusted_repositories=$(jq -c 'map(.repository)' "$script_dir/trusted-repositories.json")

common_env=(
  "CAPTURE_DIR=$test_root/capture"
  "MBX_CACHE_TEST_REPOSITORIES=$trusted_repositories"
  "MBX_CACHE_DATABASE_PASSWORD=database_password_123456"
  "MBX_CACHE_IMAGE=ghcr.io/jdx/mbx-cache@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  "OVH_SSH_HOST=mbx-cache-prod.tailnet.example"
  "PATH=$test_root/bin:$PATH"
  "R2_ACCESS_KEY_ID=r2-access-key"
  "R2_SECRET_ACCESS_KEY=r2-secret-key"
  "SHOULD_NOT_COPY=must-not-be-forwarded"
  "TERRAFORM_COMMAND=fake-terraform"
)

if env "${common_env[@]}" "$script_dir/deploy.sh" --dry-run >/dev/null 2>&1; then
  echo "deploy.sh accepted a missing OVH_SSH_SOURCE_CIDR" >&2
  exit 1
fi

if env "${common_env[@]}" \
  MBX_CACHE_IMAGE=ghcr.io/jdx/mbx-cache:0.1.0 \
  OVH_SSH_SOURCE_CIDR=203.0.113.10/32 \
  "$script_dir/deploy.sh" --dry-run >/dev/null 2>&1; then
  echo "deploy.sh accepted an image that was not pinned by digest" >&2
  exit 1
fi

invalid_repositories="$test_root/invalid-repositories.json"
printf '%s\n' '[{"repository":"jdx/mise","repository_owner_id":"216188"},{"repository":"jdx/mise","repository_owner_id":"216188"}]' >"$invalid_repositories"
if env "${common_env[@]}" \
  MBX_CACHE_GITHUB_REPOSITORIES_FILE="$invalid_repositories" \
  OVH_SSH_SOURCE_CIDR=203.0.113.10/32 \
  "$script_dir/deploy.sh" --dry-run >/dev/null 2>&1; then
  echo "deploy.sh accepted duplicate trusted repositories" >&2
  exit 1
fi

env "${common_env[@]}" \
  OVH_SSH_PORT=2222 \
  OVH_SSH_SOURCE_CIDR=203.0.113.10/32 \
  "$script_dir/deploy.sh" --dry-run

grep -Fxq 'bootstrap' "$test_root/capture/args"
grep -Fxq 'remote' "$test_root/capture/args"
grep -Fxq 'ubuntu@mbx-cache-prod.tailnet.example' "$test_root/capture/args"
grep -Fxq 'packages,files,services,firewall,compose' "$test_root/capture/args"
grep -Fxq -- '--port' "$test_root/capture/args"
grep -Fxq '2222' "$test_root/capture/args"
grep -Fq -- '--dry-run' "$test_root/capture/args"
if grep -Eq 'database_password|r2-(access|secret)-key' "$test_root/capture/args"; then
  echo "secret value appeared in mise arguments" >&2
  exit 1
fi

project_dir=$(<"$test_root/capture/project-dir")
if [[ -e $project_dir ]]; then
  echo "temporary bootstrap project was not removed: $project_dir" >&2
  exit 1
fi
