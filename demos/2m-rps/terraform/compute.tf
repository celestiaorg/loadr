# The whole fleet runs on SPOT (one-time requests, terminate on interruption).
# An interrupted agent mid-run is survivable: loadr's default agent-loss
# policy is `continue` — the remaining agents keep their share and the run
# summary notes the reduced fleet. An interrupted controller or target is a
# re-run, which is fine for a demo that costs cents per minute.

locals {
  # Graviton families → arm64 AMI + aarch64 binary; anything else → x86_64.
  arm_families = "^(c[678]g|m[678]g|r[678]g|t4g)"

  fleets = {
    controller = var.controller_instance_type
    agent      = var.agent_instance_type
    target     = var.target_instance_type
  }
  fleet_arch = { for k, t in local.fleets : k => can(regex(local.arm_families, t)) ? "arm64" : "x86_64" }

  binary_url = {
    for arch in ["arm64", "x86_64"] : arch => format(
      "https://github.com/%s/releases/%s/loadr-%s-unknown-linux-gnu.tar.gz",
      var.github_repo,
      var.loadr_version == "" ? "latest/download" : "download/v${var.loadr_version}",
      arch == "arm64" ? "aarch64" : "x86_64",
    )
  }

  placement_group = var.enable_placement_group ? aws_placement_group.cluster[0].name : null
}

data "aws_ssm_parameter" "al2023_ami" {
  for_each = toset(["arm64", "x86_64"])
  name     = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${each.key}"
}

# --- controller --------------------------------------------------------------

resource "aws_instance" "controller" {
  ami                    = data.aws_ssm_parameter.al2023_ami[local.fleet_arch.controller].value
  instance_type          = var.controller_instance_type
  subnet_id              = aws_subnet.demo.id
  vpc_security_group_ids = [aws_security_group.fleet.id, aws_security_group.controller_ui.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  placement_group        = local.placement_group

  instance_market_options {
    market_type = "spot"
    spot_options {
      spot_instance_type             = "one-time"
      instance_interruption_behavior = "terminate"
    }
  }

  user_data = templatefile("${path.module}/userdata/controller.sh.tpl", {
    binary_url = local.binary_url[local.fleet_arch.controller]
  })

  tags = { Name = "loadr-2m-controller", Role = "controller" }
}

# --- agents ------------------------------------------------------------------

resource "aws_instance" "agent" {
  count                  = var.agent_count
  ami                    = data.aws_ssm_parameter.al2023_ami[local.fleet_arch.agent].value
  instance_type          = var.agent_instance_type
  subnet_id              = aws_subnet.demo.id
  vpc_security_group_ids = [aws_security_group.fleet.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  placement_group        = local.placement_group

  instance_market_options {
    market_type = "spot"
    spot_options {
      spot_instance_type             = "one-time"
      instance_interruption_behavior = "terminate"
    }
  }

  user_data = templatefile("${path.module}/userdata/agent.sh.tpl", {
    binary_url    = local.binary_url[local.fleet_arch.agent]
    controller_ip = aws_instance.controller.private_ip
    agent_index   = count.index
  })

  tags = { Name = "loadr-2m-agent-${count.index}", Role = "agent" }
}

# --- targets -----------------------------------------------------------------

resource "aws_instance" "target" {
  count                  = var.target_count
  ami                    = data.aws_ssm_parameter.al2023_ami[local.fleet_arch.target].value
  instance_type          = var.target_instance_type
  subnet_id              = aws_subnet.demo.id
  vpc_security_group_ids = [aws_security_group.fleet.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name
  placement_group        = local.placement_group

  instance_market_options {
    market_type = "spot"
    spot_options {
      spot_instance_type             = "one-time"
      instance_interruption_behavior = "terminate"
    }
  }

  user_data = templatefile("${path.module}/userdata/target.sh.tpl", {
    main_go = file("${path.module}/../target/main.go")
  })

  tags = { Name = "loadr-2m-target-${count.index}", Role = "target" }
}
