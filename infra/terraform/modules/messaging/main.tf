variable "project_name" {
  type    = string
  default = "event-driven-arch"
}

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# SNS Topic
resource "aws_sns_topic" "orders_topic" {
  name = "orders_topic"

  sqs_success_feedback_role_arn    = data.aws_iam_role.lab_role.arn
  sqs_success_feedback_sample_rate = 100
  sqs_failure_feedback_role_arn    = data.aws_iam_role.lab_role.arn
}

# -------------------------------------------------------------------------
# Billing SQS and DLQ
# -------------------------------------------------------------------------
resource "aws_sqs_queue" "billing_dlq" {
  name = "billing_dlq"
}

resource "aws_sqs_queue" "billing_queue" {
  name = "billing_queue"
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.billing_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_policy" "billing_queue_policy" {
  queue_url = aws_sqs_queue.billing_queue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.billing_queue.arn
        Condition = {
          ArnEquals = { "aws:SourceArn": aws_sns_topic.orders_topic.arn }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "billing_sub" {
  topic_arn = aws_sns_topic.orders_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.billing_queue.arn
  filter_policy = jsonencode({
    event_type = ["OrderCreated"]
  })
}

# -------------------------------------------------------------------------
# Inventory SQS and DLQ
# -------------------------------------------------------------------------
resource "aws_sqs_queue" "inventory_dlq" {
  name = "inventory_dlq"
}

resource "aws_sqs_queue" "inventory_queue" {
  name = "inventory_queue"
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.inventory_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_policy" "inventory_queue_policy" {
  queue_url = aws_sqs_queue.inventory_queue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.inventory_queue.arn
        Condition = {
          ArnEquals = { "aws:SourceArn": aws_sns_topic.orders_topic.arn }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "inventory_sub" {
  topic_arn = aws_sns_topic.orders_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.inventory_queue.arn
  filter_policy = jsonencode({
    event_type = ["OrderCreated"]
  })
}

# -------------------------------------------------------------------------
# Shipping SQS and DLQ
# -------------------------------------------------------------------------
resource "aws_sqs_queue" "shipping_dlq" {
  name = "shipping_dlq"
}

resource "aws_sqs_queue" "shipping_queue" {
  name = "shipping_queue"
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.shipping_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue_policy" "shipping_queue_policy" {
  queue_url = aws_sqs_queue.shipping_queue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "sns.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.shipping_queue.arn
        Condition = {
          ArnEquals = { "aws:SourceArn": aws_sns_topic.orders_topic.arn }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "shipping_sub" {
  topic_arn = aws_sns_topic.orders_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.shipping_queue.arn
  filter_policy = jsonencode({
    event_type = ["OrderCreated"]
  })
}

# -------------------------------------------------------------------------
# Outputs
# -------------------------------------------------------------------------
output "orders_topic_arn" {
  value = aws_sns_topic.orders_topic.arn
}

output "billing_queue_url" {
  value = aws_sqs_queue.billing_queue.url
}

output "billing_queue_arn" {
  value = aws_sqs_queue.billing_queue.arn
}

output "inventory_queue_url" {
  value = aws_sqs_queue.inventory_queue.url
}

output "inventory_queue_arn" {
  value = aws_sqs_queue.inventory_queue.arn
}

output "shipping_queue_url" {
  value = aws_sqs_queue.shipping_queue.url
}

output "shipping_queue_arn" {
  value = aws_sqs_queue.shipping_queue.arn
}
