output "controller_public_ip" {
  value = aws_instance.controller.public_ip
}

output "controller_ui_url" {
  description = "Open in a browser; also the endpoint for `loadr run --controller`."
  value       = "http://${aws_instance.controller.public_ip}:6464"
}

output "submit_endpoint" {
  description = "Pass to `loadr run --controller <this> plans/demo-2m.yaml`."
  value       = "${aws_instance.controller.public_ip}:6464"
}

output "target_private_ips" {
  description = "Consumed by scripts/render-plans.sh to shard scenarios across targets."
  value       = aws_instance.target[*].private_ip
}

output "agent_private_ips" {
  value = aws_instance.agent[*].private_ip
}
