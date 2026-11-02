output "ecs_cluster_name" {
  description = "The name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "marketplace_task_definition_arn" {
  description = "ARN of Marketplace API Task Definition"
  value       = aws_ecs_task_definition.marketplace_api.arn
}

output "inventory_task_definition_arn" {
  description = "ARN of Inventory API Task Definition"
  value       = aws_ecs_task_definition.inventory_api.arn
}

output "cloudwatch_log_groups" {
  description = "Configured CloudWatch Log Group names"
  value = {
    marketplace = aws_cloudwatch_log_group.marketplace_api.name
    inventory   = aws_cloudwatch_log_group.inventory_api.name
    promotions  = aws_cloudwatch_log_group.promotions_api.name
    batch       = aws_cloudwatch_log_group.batch_node.name
    streaming   = aws_cloudwatch_log_group.streaming_node.name
  }
}
