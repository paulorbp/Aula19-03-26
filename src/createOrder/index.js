const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);
const snsClient = new SNSClient({});
const sqsClient = new SQSClient({});

exports.handler = async (event) => {
    console.log("Event:", JSON.stringify(event));

    try {
        const { email, ...orderData } = JSON.parse(event.body);

        // Validation
        if (!orderData.order_id || !orderData.customer_id || !orderData.items || orderData.items.length === 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Validation failed: order_id, customer_id, and items are required." })
            };
            throw error;
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
                return {
                    statusCode: 409,
                    body: JSON.stringify({ message: "Order already exists." })
                };
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

        // Send to DLQ
        if (process.env.DLQ_URL) {
            try {
                const dlqParams = {
                    QueueUrl: process.env.DLQ_URL,
                    MessageBody: JSON.stringify({
                        error: error.message,
                        stack: error.stack,
                        event: event,
                        timestamp: new Date().toISOString()
                    })
                };
                await sqsClient.send(new SendMessageCommand(dlqParams));
                console.log(`Error sent to DLQ: ${process.env.DLQ_URL}`);
            } catch (dlqError) {
                console.error(`Failed to send error to DLQ (${process.env.DLQ_URL}):`, dlqError);
            }
        } else {
            console.warn("DLQ_URL environment variable is not set. Skipping DLQ send.");
        }

        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal Server Error", error: error.message })
        };
    }
};
