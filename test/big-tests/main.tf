terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
  dynamic "filter" {
    for_each = var.availability_zone == null ? [] : [var.availability_zone]
    content {
      name   = "availability-zone"
      values = [filter.value]
    }
  }
}

data "aws_ssm_parameter" "ubuntu_arm64" {
  name = "/aws/service/canonical/ubuntu/server/resolute/stable/current/arm64/hvm/ebs-gp3/ami-id"
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name_prefix        = "genomicsqlite-big-tests-"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

# The minimum AWS-documented permissions for Session Manager. These APIs do
# not support resource-level restrictions; no S3 or Parameter Store access is
# granted to the instance. The person starting a session needs their own IAM
# permission to do so.
data "aws_iam_policy_document" "session_manager" {
  statement {
    actions = [
      "ssm:UpdateInstanceInformation",
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "session_manager" {
  name   = "session-manager"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.session_manager.json
}

resource "aws_iam_instance_profile" "instance" {
  name_prefix = "genomicsqlite-big-tests-"
  role        = aws_iam_role.instance.name
}

resource "aws_security_group" "instance" {
  name_prefix = "genomicsqlite-big-tests-"
  description = "Outbound access for GenomicSQLite big tests; access via Session Manager"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "runner" {
  ami                         = data.aws_ssm_parameter.ubuntu_arm64.value
  instance_type               = "c7gd.2xlarge"
  subnet_id                   = sort(data.aws_subnets.default.ids)[0]
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.instance.id]
  iam_instance_profile        = aws_iam_instance_profile.instance.name

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 50
    encrypted             = true
    delete_on_termination = true
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  user_data = templatefile("${path.module}/user-data.sh.tftpl", {
    scratch_setup = filebase64("${path.module}/scratch-setup.sh")
  })
  user_data_replace_on_change = true

  tags = {
    Name = "genomicsqlite-big-tests"
  }

  depends_on = [aws_iam_role_policy.session_manager]
}

variable "region" {
  description = "AWS region containing the default VPC and public Ubuntu AMI parameter."
  type        = string
  default     = "us-west-2"
}

variable "availability_zone" {
  description = "Optional availability zone for its default subnet; otherwise select a default subnet automatically."
  type        = string
  default     = null
}

output "instance_id" {
  value = aws_instance.runner.id
}

output "session_command" {
  description = "Connect using AWS CLI and the Session Manager plugin."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.runner.id}"
}
