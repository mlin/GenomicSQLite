#!/bin/bash
set -euo pipefail

# NVMe names are not stable: both the EBS root and the instance store use them.
# Refuse anything except exactly one disk identified by its AWS model string.
devices=()
for model_file in /sys/block/nvme*n1/device/model; do
    [[ -f "$model_file" ]] || continue
    if [[ $(<"$model_file") == "Amazon EC2 NVMe Instance Storage"* ]]; then
        disk_dir=$(dirname "$(dirname "$model_file")")
        devices+=("/dev/$(basename "$disk_dir")")
    fi
done
if (( ${#devices[@]} != 1 )); then
    echo "Expected exactly one NVMe instance-store disk; found ${#devices[@]}" >&2
    exit 1
fi
device=${devices[0]}

mkdir -p /scratch
if mountpoint -q /scratch; then
    [[ $(findmnt -n -o MAJ:MIN --target /scratch) == "$(lsblk -dn -o MAJ:MIN "$device")" ]]
else
    # Only format an empty, unpartitioned device. Preserve the filesystem on
    # reboot, and refuse unexpected signatures instead of destroying them.
    if [[ $(lsblk -nr -o TYPE "$device") != disk ]]; then
        echo "Instance-store disk has partitions; refusing to format $device" >&2
        exit 1
    fi
    signatures=$(wipefs --noheadings --output TYPE "$device")
    if [[ -z "$signatures" ]]; then
        mkfs.ext4 -m 0 -L gsql-scratch "$device"
    elif [[ "$signatures" != ext4 ]] || [[ $(blkid -s TYPE -o value "$device") != ext4 ]] ||
         [[ $(blkid -s LABEL -o value "$device") != gsql-scratch ]]; then
        echo "Unexpected filesystem or signatures on $device; refusing to format" >&2
        exit 1
    fi
    mount "$device" /scratch
fi

install -d -m 0755 /scratch/docker /scratch/containerd
install -d -o ubuntu -g ubuntu -m 0755 /scratch/miniwdl /scratch/miniwdl/runs \
    /scratch/miniwdl/download-cache /scratch/miniwdl/call-cache
