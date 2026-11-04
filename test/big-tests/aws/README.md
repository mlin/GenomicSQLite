# Big tests on one EC2 instance

After each use, update this runbook with what will make future runs smoother and
repeatable. Keep it concise and broadly applicable; avoid unnecessary detail
and instructions that depend on incidental choices from a particular run.

This Terraform configuration runs the [big-test WDL](../genomicsqlite_big_tests.wdl)
with miniwdl on an on-demand `c7gd.4xlarge` in the default VPC. The instance
has 16 Arm64 vCPUs, 32 GiB RAM, a 950 GB NVMe instance store, and a separate
50 GiB encrypted gp3 root volume. The default region is `us-west-2`.

Allow several hours and at least 550 GiB of scratch space for the default
inputs, which occupy about 33 GiB; actual scratch usage can be higher,
especially during the SAM QNAME sort. The WDL requests 16 CPUs per task, so
miniwdl runs one at a time, although a task may use fewer CPUs.

## Deploy and connect

The configuration uses Canonical's public Ubuntu 26.04 Arm64 AMI, a default
VPC and subnet, internet access and a public IPv4 address. It creates an
outbound-only security group and an instance role for Session Manager and
CloudWatch metrics; it does not require SSH keys or account-specific IDs. The
caller needs Terraform's EC2 and IAM permissions (including `iam:PassRole`),
Session Manager permission, and the AWS CLI Session Manager plugin. Set
`region` or `availability_zone` with Terraform variables if needed.

From the repository root:

```sh
cd test/big-tests/aws
terraform init
terraform plan
terraform apply
# Run the command printed by:
terraform output -raw session_command
```

On the instance, wait for bootstrap and enter a fresh Ubuntu login so Docker
group membership takes effect:

```sh
cloud-init status --wait
sudo -iu ubuntu
findmnt /scratch
df -h /scratch
docker info --format '{{.DockerRootDir}}'
grep '^root' /etc/containerd/config.toml
miniwdl --version
```

The instance-store device should be mounted at `/scratch`, with Docker at
`/scratch/docker` and containerd at `/scratch/containerd`. The scratch setup
formats a blank instance store, preserves its filesystem on reboot, and refuses
unexpected disk layouts. Docker and containerd depend on that setup. If boot
fails, inspect `journalctl -u genomicsqlite-scratch`,
`journalctl -u genomicsqlite-downloader`, and `/var/log/cloud-init-output.log`.
Docker group membership grants root-equivalent access.

## Monitor in CloudWatch

Bootstrap installs the CloudWatch agent and publishes host-wide CPU, memory,
`/scratch` capacity, and block-device I/O metrics every 10 seconds to the
`GenomicSQLite/BigTests` namespace. In CloudWatch Metrics, select that
namespace, filter by the instance ID printed by Terraform, and graph CPU
`usage_active`/`usage_iowait`, memory `used_percent`/`available_percent`,
`disk_used_percent`/`disk_free` for `/scratch`, and `diskio_*` for the NVMe
instance-store device shown by `findmnt /scratch` and `lsblk`. For disk I/O
byte and time counters, use CloudWatch's **Sum** statistic for each period;
for CPU, memory and capacity use **Average** or **Maximum** as appropriate.
These are host metrics rather than per-task measurements.

Check `systemctl status amazon-cloudwatch-agent` and the agent log under
`/opt/aws/amazon-cloudwatch-agent/logs/` if metrics do not appear. The
instance role can publish only to this namespace and can describe EC2 tags;
it cannot write CloudWatch logs. The 10-second samples are billed as custom
metrics and may create multiple series for disk devices. CloudWatch keeps
sub-minute data for only three hours, then rolls it up to one-minute data;
export during the run if the full 10-second history matters. The ordinary
`AWS/EC2` CPU metric remains available separately at five-minute resolution.

For a durable local 10-second copy spanning a multi-hour run, optionally use
`sar -A -o /scratch/miniwdl/sysstat.sa 10` in a detached tmux session (install
`sysstat` if needed). Stop it with Ctrl-C after the run. Its raw file includes
the hostname and must be sanitized before publication.

## Run

Choose the WDL revision and source revision deliberately. They are separate:
the WDL's `git_revision` input selects the GenomicSQLite source to build and
defaults to the moving `main` branch. The build task clones
`https://github.com/mlin/GenomicSQLite.git`, so the chosen source commit must
be available there. The following example resolves `main` to a commit; fetch
the WDL revision from its source if it is not in the clone.

```sh
git clone https://github.com/mlin/GenomicSQLite.git
cd GenomicSQLite
# Fetch if necessary, then check out the desired WDL revision.
git rev-parse HEAD                    # record the WDL revision
miniwdl check test/big-tests/genomicsqlite_big_tests.wdl
(cd /scratch/miniwdl && miniwdl run_self_test)
```

Run in tmux so an interrupted Session Manager connection does not stop the
workflow. Within the tmux session, record the console output and exit status:

```sh
tmux new-session -s big-tests
# Inside tmux, from the GenomicSQLite checkout:
set -o pipefail
source_rev=$(git ls-remote origin refs/heads/main | cut -f1)
test -n "$source_rev" || { echo 'Could not resolve source revision' >&2; exit 1; }
printf 'source revision: %s\n' "$source_rev"
miniwdl run --no-color --dir /scratch/miniwdl/runs \
  test/big-tests/genomicsqlite_big_tests.wdl "git_revision=$source_rev" \
  2>&1 | tee /scratch/miniwdl/big-tests-console.log
printf '%s\n' "${PIPESTATUS[0]}" > /scratch/miniwdl/big-tests.exit
```

Detach with `Ctrl-b`, then `d`; reconnect with `tmux attach -t big-tests`.
Monitor `df -h /scratch`, the console log, and the task logs in the run
directory. The workflow prints query results but does not assert their expected
values, so a successful exit is a stability check, not a full correctness
proof. The `.dbi` 404 in the no-index web test is expected fallback behavior.

miniwdl 1.16.0 uses the Docker backend and the experimental `inlineDockerfile`
extension. Bootstrap builds an Arm64 aria2/AWS downloader image because the
default image is amd64-only; Google Storage uses the multi-architecture Google
Cloud CLI image. Inputs and successful calls are cached under
`/scratch/miniwdl`; call-cache entries reference earlier run directories, so
keep those directories for retries. Cache lookup does not validate mutable
remote URIs or image tags; use immutable revisions or disable cache for a
fresh comparison. The public GCS input and S3 CRAM require internet access;
the CRAM is in `us-east-1`.

## Preserve results and clean up

Before teardown, save the workflow and task logs, inputs/outputs, WDL and
source revisions, timings, system configuration, exported CloudWatch metrics,
and a run-directory size inventory. Omit the large databases and inputs if they
are throwaway. A sanitized example is in [runs](../runs/). Check logs
and sysstat data for account IDs, hostnames, network details, and other
identifiers before publishing. The instance role has no S3 upload permissions;
use a separately authorized transfer or add narrowly scoped permissions.

Instance-store contents—including Docker images, caches, logs, and run
files—are lost on stop, hibernation, termination, or host replacement. Preserve
anything needed **before** stopping the instance. Terraform state is local and
may contain infrastructure details. When finished, from the repository root:

```sh
cd test/big-tests/aws
terraform destroy
```
