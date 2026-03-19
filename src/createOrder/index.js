const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, PutCommand } = require('@aws-sdk/lib-dynamodb');
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns');

const ddbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(ddbClient);
const snsClient = new SNSClient({});

exports.handler = async (event) => {
    console.log("Event:", JSON.stringify(event));
    
    try {
        const body = JSON.parse(event.body);
        
        // Validation
        if (!body.order_id || !body.customer_id || !body.items || body.items.length === 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({ message: "Validation failed: order_id, customer_id, and items are required." })
            };
        }

        // Persist in Order DynamoDB
        const putParams = {
            TableName: process.env.ORDER_TABLE,
            Item: body,
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
            Message: JSON.stringify(body),
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
            body: JSON.stringify({ message: "Order created successfully", order_id: body.order_id })
        };
    } catch (error) {
        console.error("Error:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Internal Server Error", error: error.message })
        };
    }
};
