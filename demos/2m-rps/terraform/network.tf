# Single AZ on purpose: no cross-AZ data charges, flat latency, and the demo
# measures the generator/target, not AWS's backbone. One public subnet so
# every box can pull its binary without a NAT gateway; nothing listens to the
# internet except the controller UI (admin_cidr only). Access is SSM-only.

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "demo" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = { Name = "loadr-2m-demo" }
}

resource "aws_internet_gateway" "demo" {
  vpc_id = aws_vpc.demo.id
}

resource "aws_subnet" "demo" {
  vpc_id                  = aws_vpc.demo.id
  cidr_block              = "10.42.0.0/20"
  availability_zone       = data.aws_availability_zones.available.names[0]
  map_public_ip_on_launch = true
  tags                    = { Name = "loadr-2m-demo" }
}

resource "aws_route_table" "demo" {
  vpc_id = aws_vpc.demo.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.demo.id
  }
}

resource "aws_route_table_association" "demo" {
  subnet_id      = aws_subnet.demo.id
  route_table_id = aws_route_table.demo.id
}

resource "aws_placement_group" "cluster" {
  count    = var.enable_placement_group ? 1 : 0
  name     = "loadr-2m-demo"
  strategy = "cluster"
}

# Everything talks to everything inside the fleet; only the controller UI is
# reachable from outside, and only from admin_cidr.
resource "aws_security_group" "fleet" {
  name        = "loadr-2m-demo-fleet"
  description = "loadr 2M rps demo - intra-fleet traffic"
  vpc_id      = aws_vpc.demo.id

  ingress {
    description = "all intra-fleet"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "controller_ui" {
  name        = "loadr-2m-demo-controller-ui"
  description = "loadr 2M rps demo - controller UI/API from admin only"
  vpc_id      = aws_vpc.demo.id

  ingress {
    description = "controller UI/API (loadr run --controller, browser)"
    from_port   = 6464
    to_port     = 6464
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }
}
