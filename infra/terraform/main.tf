terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Nota: Bloco de backend do Terraform não aceita variáveis. 
  # A unicidade do arquivo de estado deve ser garantida via configuração no init ou 
  # manualmente no nome da key/bucket.
  backend "s3" {
    key    = "state/terraform.tfstate.events.paulorbp" # Substituir ACCOUNT_ID dinamicamente no CI/CD se necessário
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

module "database" {
  source = "./modules/database"
}

module "messaging" {
  source             = "./modules/messaging"
  notification_email = var.notification_email
}

module "compute" {
  source               = "./modules/compute"
  order_table_name     = module.database.order_table_name
  billing_table_name   = module.database.billing_table_name
  inventory_table_name = module.database.inventory_table_name
  shipping_table_name  = module.database.shipping_table_name
  orders_topic_arn     = module.messaging.orders_topic_arn
  billing_queue_arn    = module.messaging.billing_queue_arn
  inventory_queue_arn  = module.messaging.inventory_queue_arn
  shipping_queue_arn   = module.messaging.shipping_queue_arn
  create_order_dlq_url = module.messaging.create_order_dlq_url
}

module "api_gateway" {
  source                   = "./modules/api_gateway"
  create_order_lambda_arn  = module.compute.create_order_lambda_arn
  create_order_lambda_name = module.compute.create_order_lambda_name
}

output "api_endpoint" {
  value = module.api_gateway.api_endpoint
}

output "order_endpoint" {
  value = module.api_gateway.order_endpoint
}
