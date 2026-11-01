# Big tests on one EC2 instance

This directory contains the big-test WDL and a small Terraform configuration for
running it with miniwdl on an on-demand `c7gd.2xlarge` in the default VPC. The
instance has eight Arm64 vCPUs, 16 GiB RAM, one 474 GB NVMe instance-store disk,
and a separate 50 GiB encrypted gp3 root volume. The default region is
`us-west-2`; `-var='region=...'` can select another region with a default VPC,
default subnet, and this instance type. If needed, use
`-var='availability_zone=us-west-2a'` to choose an availability zone; otherwise
Terraform selects a default subnet. The configuration does not include AWS
account IDs, VPC IDs, credentials, or SSH keys.

Terraform creates an outbound-only security group and an instance profile with
the AWS-documented minimum permissions for Systems Manager Session Manager.
There is no inbound SSH rule and no S3 access in the instance role. The caller
needs Terraform's usual EC2 and IAM permissions, including permission to pass
the role, and separate permission to start a Session Manager session. The AWS
CLI's Session Manager plugin is needed on the client. This uses the standard
Ubuntu 26.04 Arm64 AMI from Canonical's public SSM parameter; a default subnet
with internet access and an available public IPv4 address are required.

Review the plan before creating billable resources:

```sh
terraform init
terraform plan
terraform apply
```

Connect with the command printed by `terraform output -raw session_command`.
The first boot installs Docker, containerd, and miniwdl, so wait for cloud-init
to finish before starting a run:

```sh
cloud-init status --wait
sudo -iu ubuntu
docker info --format '{{.DockerRootDir}}'
miniwdl --version
```

Docker's root should be `/scratch/docker`. Both Docker and containerd depend on
the scratch setup service, and containerd's data root is `/scratch/containerd`.
The service identifies the instance store by its NVMe model, formats it only
if it has no existing signatures or partitions, and mounts it at `/scratch`.
It preserves the filesystem on reboot and refuses unexpected disk layouts.
Inspect failures with `journalctl -u genomicsqlite-scratch` and
`/var/log/cloud-init-output.log`. Docker group membership takes effect in a new
login; `sudo -iu ubuntu` provides one. Membership in that group permits root
access on the host.

Clone this repository and check out the WDL revision to run, for example:

```sh
git clone https://github.com/mlin/GenomicSQLite.git
cd GenomicSQLite
# Check out the desired WDL revision here.
miniwdl check test/big-tests/genomicsqlite_big_tests.wdl
miniwdl run_self_test
miniwdl run --dir /scratch/miniwdl/runs test/big-tests/genomicsqlite_big_tests.wdl
```

The WDL's `git_revision` input separately chooses which GenomicSQLite revision
the build task checks out; its default is `main`. Set it explicitly when the
tested source revision matters. The WDL uses miniwdl's `inlineDockerfile`
extension; bootstrap installs miniwdl 1.16.0 and the Docker backend. Both the
instance OS and the task images use Ubuntu 26.04.

The default miniwdl downloader image is amd64-only. Bootstrap builds a local
Arm64 image containing aria2 and AWS CLI and configures both HTTP/FTP and S3
downloads to use it. An enabled systemd service rebuilds it if the instance
store is lost after a stop/start; image building requires internet access. For
Google Storage, miniwdl uses Google's multi-architecture Cloud CLI image.

The system miniwdl configuration enables call and download caching under
`/scratch/miniwdl`, and places task temporary files on the run filesystem.
The call cache references files in earlier run directories, so keep those
directories while using the cache. Storage is finite: the default CRAM and VCF
alone occupy about 33 GiB, before the intermediate BAM and databases. Check
`df -h /scratch` during the run. The GCS VCF and other public downloads require
internet access; the public S3 CRAM resides in `us-east-1`.

Instance-store contents, including Docker images, caches, and run files, are
lost on stop, hibernation, termination, or host replacement. Copy anything to
retain to durable storage before stopping the instance. The instance profile
does not grant bucket access; add narrowly scoped permissions separately if
you intend to upload to S3. Terraform state is local by default and may contain
infrastructure details; retain it securely until cleanup. When finished:

```sh
terraform destroy
```
