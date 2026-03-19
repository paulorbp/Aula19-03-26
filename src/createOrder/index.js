const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);
const snsClient = new SNSClient({});

class ResponseError extends Error {
    constructor(statusCode, message) {
        super(message);
        this.statusCode = statusCode;
        this.name = "ResponseError";
    }
}

exports.handler = async (event) => {
    console.log("Event:", JSON.stringify(event));

    try {
        const { email, ...orderData } = JSON.parse(event.body);

        // Validation
        if (!orderData.order_id || !orderData.customer_id || !orderData.items || orderData.items.length === 0) {
            throw new ResponseError(400, "Validation failed: order_id, customer_id, and items are required.");
        }

        // Persist in Order DynamoDB
        const putParams = {
            TableName: process.env.ORDER_TABLE,
            Item: orderData,
            ConditionExpression: "attribute_not_exists(order_id)"
        };

        try {
            await docClient.send(new PutCommand(putParams));
        } catch (error) {
            if (error.name === 'ConditionalCheckFailedException') {
                throw new ResponseError(409, "Order already exists.");
            }
            throw error;
        }

        // Publish to SNS
        const publishParams = {
            TopicArn: process.env.SNS_TOPIC_ARN,
            Message: JSON.stringify({
                default: JSON.stringify(orderData),
                email: `Novo pedido criado!\n\nID do Pedido: ${orderData.order_id}\nCliente: ${orderData.customer_id}${email ? `\nEmail do Cliente: ${email}` : ''}\n\nObrigado por comprar conosco!`
            }),
            MessageStructure: "json",
            Subject: "Confirmação de Pedido",
            MessageAttributes: {
                "event_type": {
                    DataType: "String",
                    StringValue: "OrderCreated"
                }
            }
        };

        await snsClient.send(new PublishCommand(publishParams));

        return {
            statusCode: 201,
            body: JSON.stringify({ message: "Order created successfully", order_id: orderData.order_id })
        };
    } catch (error) {
        console.error("Caught Exception:", error.name, error.message);
        if (error.stack) console.error(error.stack);

        // Send to SNS for Error DLQ routing
        if (process.env.SNS_TOPIC_ARN) {
            try {
                const errorParams = {
                    TopicArn: process.env.SNS_TOPIC_ARN,
                    Message: JSON.stringify({
                        default: JSON.stringify({
                            error: error.message,
                            stack: error.stack,
                            event: event,
                            timestamp: new Date().toISOString()
                        }),
                        email: `Falha ao processar pedido!\n\nErro: ${error.message}\nVerifique a DLQ para mais detalhes.`
                    }),
                    MessageStructure: "json",
                    Subject: "Erro no Processamento de Pedido",
                    MessageAttributes: {
                        "event_type": {
                            DataType: "String",
                            StringValue: "OrderError"
                        }
                    }
                };
                await snsClient.send(new PublishCommand(errorParams));
                console.log(`Error event sent to SNS: ${process.env.SNS_TOPIC_ARN}`);
            } catch (snsError) {
                console.error(`Failed to send error event to SNS (${process.env.SNS_TOPIC_ARN}):`, snsError);
            }
        } else {
            console.warn("SNS_TOPIC_ARN environment variable is not set. Skipping SNS send for error.");
        }

        const statusCode = error.statusCode || 500;
        const bodyContent = error.statusCode 
            ? { message: error.message }
            : { message: "Internal Server Error", error: error.message };

        return {
            statusCode: statusCode,
            body: JSON.stringify(bodyContent)
        };
    }
};
