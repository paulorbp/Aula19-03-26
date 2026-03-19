# Events - AWS Event-Driven Architecture

Este projeto implementa uma arquitetura baseada em eventos (EDA) na AWS, utilizando **Terraform** para infraestrutura como código e **GitHub Actions** para CI/CD automático.

O sistema processa pedidos de forma assíncrona, distribuindo a carga entre diferentes serviços (Faturamento, Estoque e Entrega) através de um padrão Fan-out com SNS e SQS.

---

## 🏗️ Arquitetura

O diagrama detalhado da arquitetura e a descrição dos fluxos podem ser encontrados em [docs/arquitetura.md](docs/arquitetura.md).

### Componentes Principais
- **API Gateway**: Ponto de entrada para criação de pedidos via HTTP POST.
- **Lambda Functions**:
  - `createOrder`: Valida pedidos, persiste no DynamoDB e publica no SNS.
  - `billingRegister`: Processa pagamentos (via SQS).
  - `inventoryRegister`: Atualiza estoque (via SQS).
  - `shippingRegister`: Inicia fluxo de entrega (via SQS).
- **Mensageria**:
  - **SNS (orders_topic)**: Distribui eventos de pedidos criados.
  - **SQS (billing, inventory, shipping)**: Filas individuais para processamento resiliente.
  - **DLQs (Dead Letter Queues)**: Capturam falhas de processamento em todas as filas e na Lambda principal.
- **DynamoDB**: Tabelas NoSQL para persistência de pedidos e dados de domínios específicos.

---

## 📂 Estrutura do Projeto

```text
.
├── data/               # Dados para semente (seed)
├── docs/               # Documentação técnica e diagramas
├── infra/
│   └── terraform/      # Definições de infraestrutura (HCL)
├── scripts/            # Scripts utilitários (Seed, etc.)
└── src/                # Código fonte das funções Lambda (Node.js)
```

---

## 🚀 Como Executar

### Pré-requisitos
- [Terraform](https://www.terraform.io/downloads.html) >= 1.0
- [AWS CLI](https://aws.amazon.com/cli/) configurado
- [Node.js](https://nodejs.org/) >= 22 (para scripts locais)

### 1. Provisionamento (Terraform)
Navegue até o diretório de infraestrutura:
```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

### 2. Semeando Dados (Seed)
Caso deseje popular as tabelas iniciais para teste:
```bash
# Na raiz do projeto
node scripts/seed.js
```

---

## 📡 Endpoints da API

Após o `terraform apply`, o Terraform exibirá os seguintes outputs:
- `api_endpoint`: URL base do API Gateway.
- `order_endpoint`: Endpoint direto para criação de pedidos (`POST /orders`).

### Exemplo de Payload (POST /orders)
```json
{
  "order_id": "ORD-123",
  "customer_id": "CUST-456",
  "email": "cliente@exemplo.com",
  "items": [
    { "product": "Laptop", "quantity": 1 }
  ]
}
```

---

## 🛡️ Resiliência e Monitoramento

- **Retry e DLQ**: Configurado com 3 tentativas automáticas antes de enviar mensagens falhas para as Dead Letter Queues.
- **SNS Logging**: Logs de entrega do SNS habilitados no CloudWatch para auditoria.
- **Segurança**: Uso da `LabRole` da AWS Academy e variáveis de ambiente protegidas.

---

## 🛠️ CI/CD

O projeto utiliza **GitHub Actions** (`.github/workflows/terraform.yml`) para:
1. Validar mudanças no Terraform.
2. Gerenciar o estado (`tfstate`) remotamente em um bucket S3.
3. Aplicar automaticamente as mudanças na branch `main`.
4. Para uso do Github Actions, devem ser atualizadas as seguintes variáveis no Secrets do Github:
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - AWS_SESSION_TOKEN
  - AWS_REGION
  - NOTIFICATION_EMAIL
   
