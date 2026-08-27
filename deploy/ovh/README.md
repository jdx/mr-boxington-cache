# OVH US deployment

This deployment runs Caddy, mbx-cache, and PostgreSQL on one OVHcloud VPS in
Vint Hill, Virginia. Cache blobs live in a manually created Cloudflare R2
bucket. The server is disposable; PostgreSQL is small and the much larger blob
store remains outside the server.

Terraform records or provisions the server:

- infrastructure around an existing VPS identified by IPv4 address, or one new
  monthly OVH US VPS in `US-EAST-VA`.

The R2 bucket and DNS record are deliberately created in Cloudflare by an
operator. Terraform therefore needs no Cloudflare API token, and mbx-cache can
use an Object Read & Write token scoped to only its bucket.

[`mise bootstrap remote`](https://mise.jdx.dev/bootstrap/remote.html) installs
and converges the host firewall, fail2ban, automatic security updates, Docker,
Caddy, PostgreSQL, and mbx-cache. Runtime secrets are copied only through a
protected temporary bootstrap project; they do not enter Terraform state, mise
configuration, Git, or OVH installation metadata.

## Cost profile

The entry OVH VPS currently starts around USD 4.54/month and includes an IPv4
address, daily backup, and unlimited traffic. R2 Standard storage is USD
0.015/GB-month with 10 GB-month included and free egress. Approximate
VPS-plus-storage totals are USD 5.89/month for 100 GB or USD 8.14/month for
250 GB; R2 operation charges can add to these estimates. Check current prices
and plan availability before applying the configuration.

## Prerequisites

- Terraform or OpenTofu 1.8 or newer
- mise 2026.8.2 or newer
- local `curl`, `jq`, `ssh`, and `tar` commands
- an existing Ubuntu VPS with key-based SSH access, or an OVH US account with
  credentials and a default payment method for ordering a new VPS
- when `OVH_SSH_HOST` uses a tailnet address, a server already enrolled in the
  same tailnet with TCP 22 permitted by its tailnet policy, plus a connected
  local Tailscale client and `tailscale` command; bootstrap permits SSH on the
  existing `tailscale0` interface but does not install or enroll Tailscale
- a current OVH US VPS plan code and Ubuntu image ID only when ordering a VPS
- an existing R2 bucket and Object Read & Write token restricted to that bucket
- a DNS-only Cloudflare A record for the public cache hostname
- a published mbx-cache image digest (`…@sha256:<64 hex>`; `deploy.sh` rejects a tag)

Set `TERRAFORM_COMMAND=tofu` when using OpenTofu for the deploy step.

OVH plan codes and availability change. Query the OVH US order catalog rather
than copying a stale value:

1. Create and assign a cart through the OVH API.
2. Call `GET /order/cart/{cartId}/vps` and select the current VPS-1 plan.
3. Use `US-EAST-VA` for `vps_datacenter` and `Ubuntu 24.04` for `vps_os`.

The `ovh_vps` Terraform resource requires an image ID when installing a public
SSH key. Retrieve the current Ubuntu image ID from
`GET /vps/{serviceName}/images/available` on an existing OVH VPS. This is an
OVH API limitation; the ID is then declaratively pinned for installation.

## Provision infrastructure

Copy the example variables file and edit its values:

```sh
cd deploy/ovh/terraform
cp terraform.tfvars.example terraform.tfvars
```

Set `existing_server_ipv4` to adopt a server that has already been purchased.
In that mode Terraform does not create or manage the VPS, and the OVH provider
does not need credentials. Leave it unset and provide `plan_code`, `image_id`,
and `public_ssh_key` to order a new server.

Do not switch a VPS that this configuration already manages directly into
adoption mode. The resource has `prevent_destroy` enabled, so Terraform rejects
the otherwise destructive transition. First record and verify
`terraform output -raw server_ipv4`, set `existing_server_ipv4` to that exact
address, remove only `ovh_vps.cache[0]` from state with
`terraform state rm 'ovh_vps.cache[0]'`, and then review a fresh plan before
applying. Removing the resource from state does not delete the VPS.

Export OVH provider credentials only when ordering a VPS, then review the plan
carefully.
Creating `ovh_vps.cache[0]` purchases a recurring service; an existing-server
plan must not contain that resource:

```sh
export OVH_ENDPOINT=ovh-us
export OVH_APPLICATION_KEY=...
export OVH_APPLICATION_SECRET=...
export OVH_CONSUMER_KEY=...
terraform init
terraform plan
terraform apply
```

Use a remote encrypted Terraform backend for production state. Local state and
`terraform.tfvars` are ignored by Git.

For an existing server, the Terraform state contains configuration outputs but
no managed infrastructure resources.

## Create R2 storage and DNS

Create the `mise-cache-production` R2 bucket with Standard storage in Eastern
North America, then create an R2 API token with Object Read & Write permission
scoped only to that bucket. Save its Access Key ID and Secret Access Key;
Cloudflare displays the secret only once.

Create a DNS-only A record from `cache.mise.jdx.dev` to the Terraform
`server_ipv4` output. Do not enable the Cloudflare proxy: cache blobs may be
larger than proxied request-body limits, so requests must go directly to Caddy
on the VPS.

## Deploy the service

Pin `MBX_CACHE_IMAGE` by digest so a deployment cannot silently select changed
image content:

```sh
export MBX_CACHE_IMAGE=ghcr.io/jdx/mbx-cache@sha256:<64-hex-digit-digest>
export MBX_CACHE_DATABASE_PASSWORD="$(openssl rand -hex 24)"
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export OVH_SSH_HOST="mise-cache-prod.example-tailnet.ts.net"
export OVH_SSH_SOURCE_CIDR="$(tailscale ip -4)/32"
./deploy/ovh/deploy.sh
```

`OVH_SSH_HOST` selects a private DNS name, IP address, or OpenSSH host alias
without changing the public address used by DNS. It defaults to the Terraform
`server_ipv4` output. `OVH_SSH_SOURCE_CIDR` is required: there is no world-open
default. Mise checks the active SSH connection against this rule and
`OVH_SSH_PORT` before atomically applying the nftables policy, so an incorrect
CIDR or port fails before it can lock out the operator. The declared policy
permits TCP 80/443, UDP 443 for HTTP/3, SSH from the current deployment peer,
and SSH from the Tailscale CGNAT range only when traffic arrives on
`tailscale0`.

The deploy script reads the hostname, server address, R2 endpoint, and bucket
from Terraform outputs. It creates a mode-`0700` local bootstrap project,
writes only the explicitly required runtime values into mode-`0600` source
files, and runs `mise bootstrap remote`. Mise stages the project in another
mode-`0700` temporary directory on the host, converges the protected service
environment files, and removes the staging directory. A shell trap removes the
local project on success or failure. The caller's other environment variables
are never forwarded.

Automated deployments store `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and
`MISE_CACHE_DATABASE_PASSWORD` in the repository's protected `production` GitHub
Environment; see "Names that predate the rebrand" below for why that last one
keeps its original name. Environment protection keeps these values out of
ordinary pull-request jobs. The R2 credential remains limited by its Cloudflare bucket
policy even if a workflow is compromised. Use a local password manager to
populate the same variables for an emergency operator-run deployment; never
commit their plaintext values.

Ubuntu images use the `ubuntu` SSH user by default. Override `OVH_SSH_USER`,
`OVH_SSH_PORT`, `OVH_SSH_HOST`, or `OVH_SSH_IDENTITY_FILE` when necessary;
normal OpenSSH configuration and host-key policy still apply. Additional
arguments are passed to `mise bootstrap remote`, including repeatable
`--ssh-option` values for bastions and userspace-networking proxies. The
complete remote plan can be inspected without changing the host:

```sh
./deploy/ovh/deploy.sh --dry-run
```

On apply, the wrapper updates package metadata, converges only the package,
file, service, firewall, and Compose phases, then waits up to ten minutes for
the public HTTPS status endpoint. Keep the database password in a password
manager because changing it after PostgreSQL initializes requires a database
role-password rotation.

GitHub OIDC is configured with these server-enforced grants:

- repositories listed in `trusted-repositories.json` on `main`, when the
  workflow run was initiated by the configured write actor: read/write;
- tag workflows for listed repositories: read-only;
- pull-request workflows for listed repositories: read-only;
- other push workflows for listed repositories: read-only; and
- the exact `jdx/mr-boxington-cache` production deployment workflow: read/write for
  its isolated qualification namespace.

Each trusted repository is paired with GitHub's stable numeric
`repository_owner_id`; repository names must be unique. Edit the checked-in
file to change the production allowlist, or set
`MBX_CACHE_GITHUB_REPOSITORIES_FILE` to a different JSON file for another
installation. Write grants additionally require GitHub's stable numeric
`actor_id`; it defaults to jdx's account ID, `216188`, and can be changed with
`MBX_CACHE_GITHUB_WRITE_ACTOR_ID`. Read-only grants do not restrict the actor,
so pull requests and automation initiated by other accounts can still consume
trusted cache entries without publishing new ones. Override
`MBX_CACHE_DEPLOY_GITHUB_REPOSITORY` and
`MBX_CACHE_DEPLOY_GITHUB_OWNER_ID`, and
`MBX_CACHE_DEPLOY_GITHUB_WORKFLOW_REF` together when deployment is managed by
another repository owner or workflow.

## Verify and operate

```sh
curl --fail "$(terraform -chdir=deploy/ovh/terraform output -raw cache_url)/v1/status"
ssh ubuntu@"$OVH_SSH_HOST" \
  'cd /opt/mise-cache && sudo docker compose ps'
```

The desired host state lives in `deploy/ovh/bootstrap/mise.toml`. Re-running
the deployment is convergent: mise skips matching packages and files, verifies
service state and the atomic firewall fingerprint, and recreates Compose
containers only when their effective configuration has changed.

Prometheus metrics remain available to containers at `/metrics`, but Caddy
does not expose that endpoint publicly. Prometheus collects them every five
seconds and retains up to 90 days or 2 GB, whichever bound is reached first.
Grafana and Prometheus bind only to the host loopback interface. Open the
provisioned dashboard through an SSH tunnel:

```sh
ssh -N -L 3000:127.0.0.1:3000 ubuntu@"$OVH_SSH_HOST"
```

Then visit `http://127.0.0.1:3000/d/mbx-cache/mbx-cache`. Grafana has no
local administrator or editor account; anonymous access is read-only and is
reachable only through the loopback-bound port. Dashboard and datasource
changes belong in `deploy/ovh/bootstrap/monitoring` and are applied by the next
convergent deployment. The deployment hashes those files into the affected
service definition so startup-loaded configuration changes recreate only
Prometheus or Grafana as needed. Grafana keeps no mutable state: its exact
datasource and dashboard set is reprovisioned from these files on recreation.

### Bounding storage

R2 expires the objects through a bucket lifecycle rule, and a metadata sweep
drops the rows those objects leave behind. Run the two together:

```sh
# Once, on the bucket: expire objects by age.
# Then, periodically, on the host:
docker compose exec cache mbx-cache --sweep-metadata-older-than-days 35
```

**Keep the sweep's age longer than the lifecycle's.** Storage has to delete
first so metadata follows it; reversed, the sweep drops rows for objects that
still exist and every client that wanted one recompiles for nothing.

An earlier note here warned against a lifecycle rule at all, on the grounds that
expiring objects would leave action results pointing at missing blobs. That part
is true, but it is not harmful: a client that cannot fetch a referenced blob logs
it and treats the action as a miss, so a dangling reference costs a recompile
rather than a failed build. What it does cost is a wasted round trip on every
lookup, which is what the sweep removes.

Three things this pairing cannot do. The lifecycle expires by age since upload,
not by last use, so an object served on every build is still deleted at the
cutoff and re-uploaded; the server records `last_accessed_at` on reads, but
nothing acts on it yet. Nested objects inside a directory are not examined when
deciding whether a result dangles, so a result whose output tree lost one file is
swept only once its top-level objects go. And a blob row's age is the age of the
upload that wrote the object, which the first namespace to register an object
another namespace uploaded earlier cannot know: that row starts its clock late
and can outlive the object by up to the retention window, until the following
sweep takes it.

## Names that predate the rebrand

Renaming this project from `mise-cache` to `mbx-cache` renamed only what lives
in this repository. Several identifiers name resources owned by Tailscale,
Cloudflare, GitHub, and the host itself, and those keep their original names.
Two deploys failed in a row because the repository was changed and the resource
was not, so **do not "fix" these to match the brand** unless you rename the
resource first.

| Identifier | Where it lives | Why it stays |
| --- | --- | --- |
| `MISE_CACHE_DATABASE_PASSWORD` | `production` GitHub Environment secret | Cannot easily be recreated; the workflow maps it onto `MBX_CACHE_DATABASE_PASSWORD` |
| `tag:mise-cache-ci` | Tailscale ACL policy | CI may only advertise a tag the tailnet policy defines |
| `mise-cache-prod.tail13c301.ts.net` | The host's MagicDNS name | Renaming the machine changes the name CI pings and reaches over SSH |
| `mise-cache-production` | Cloudflare R2 | Pre-existing bucket; the R2 token is scoped to this exact name, and the cache's stored blobs are in it |
| `/opt/mise-cache` | The host's filesystem | Where the running deployment already lives; a new path would stand up a second copy beside it |
| `mise-cache` (Compose project) | Docker on the host | The project name owns the containers and volumes, so changing it starts a parallel stack that contends for ports 80 and 443 |
| `mise_cache` (role and database) | PostgreSQL on the host | `POSTGRES_USER` and `POSTGRES_DB` only apply when a volume initialises, so an existing cluster keeps this role and a renamed URL cannot authenticate |
| `mise-cache.conf` | `/etc/fail2ban/jail.d` | Renaming leaves the old jail in place alongside the new one |
| `mise-cache.json`, `mise-cache.yml` | Grafana provisioning on the host | Both files would be mounted, so Grafana would show the dashboard twice |

One identifier could not be preserved: GitHub's OIDC subject embeds the
repository, so the tokens this workflow presents now say `jdx/mr-boxington-cache`. Any
external trust that was pinned to an earlier repository name has to be updated on that
side — including the Tailscale trust credential used to join the tailnet, which
is what fails if the tailnet step reports `failed to exchange JWT for access
token`.

Environment variable *names* passed between `deploy.sh` and Compose are not in
this category and do use the `MBX_CACHE_` prefix: both ends live in this
repository, so nothing outside it depends on them.

If decoupling the identifiers above from the brand becomes worthwhile, either
move them to repository variables so the values live with the infrastructure, or
migrate the host once and rename them together.
