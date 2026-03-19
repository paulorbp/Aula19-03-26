variable "aws_region" {
  description = "AWS Region to deploy to"
  type        = string
  default     = "us-east-1"
}

variable "notification_email" {
  description = "Email to receive order notifications"
  type        = string
}
