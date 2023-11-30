const AWS = require("aws-sdk");
const { Storage } = require("@google-cloud/storage");
const { SES } = require("aws-sdk");
const axios = require("axios");
require("dotenv").config();
const mailgun = require("mailgun-js");
const winston = require("winston");

const logger = winston.createLogger({
  transports: [new winston.transports.Console()],
  format: winston.format.simple(),
});

exports.handler = async (event, context) => {
  try {
    console.log("Entered serverless code");
    console.log("SNS Message:", event.Records[0].Sns.Message);
    // Extract user information from SNS message
    const snsMessage = JSON.parse(event.Records[0].Sns.Message);
    const firstName = snsMessage.firstName;
    const userEmail = snsMessage.email;
    const submissionUrl = snsMessage.submissionUrl;

    // Get variables from environment
    const gcpBucketName = process.env.BUCKET_NAME;
    const accessKey = process.env.ACCESS_KEY;
    const dynamoTableName = process.env.DYNAMODB_TABLE;

    if (!gcpBucketName || !accessKey || !dynamoTableName) {
      logger.error("Missing environment variables.");
      return;
    }

    // Action 1: Download file from URL and upload to GCP Bucket
    const fileContent = await downloadFile(submissionUrl);
    await uploadToGCPBucket(fileContent, accessKey, gcpBucketName);

    // Action 2: Send email status via Mailgun
    const emailStatus = "Success"; // or "Failure"
    await sendEmailViaMailgun(userEmail, emailStatus);

    // Action 3: Track emails sent in DynamoDB
    await trackEmailsInDynamoDB(userEmail, dynamoTableName);
  } catch (error) {
    logger.error("Error processing Lambda function:", error);
    throw error;
  }
};

// Download file from URL
const downloadFile = async (submissionUrl) => {
  logger.info("Downloading file from:", submissionUrl);
  try {
    const response = await axios.get(submissionUrl, {
      responseType: "arraybuffer",
    });
    logger.info("Response Headers:", response.headers);
    logger.info("Content Type:", response.headers["content-type"]);

    // Return the downloaded content as a buffer
    return Buffer.from(response.data);
  } catch (error) {
    logger.error("Error downloading file:", error);
    throw error;
  }
};

// Upload file to GCP Bucket
const uploadToGCPBucket = async (fileContent, accessKey, gcpBucketName) => {
  try {
    logger.info("Uploading to GCP Bucket:", gcpBucketName);
    logger.info("Content being parsed:", fileContent);

    // Parse the GCP credentials
    const credentials = JSON.parse(accessKey);

    // Create a new Storage client
    const storage = new Storage({
      projectId: credentials.project_id,
      credentials: credentials,
    });

    // Specify the target bucket and file name
    const bucket = storage.bucket(gcpBucketName);
    const fileName = "uploaded-file.txt";

    // Create a new file in the bucket and upload the content
    const file = bucket.file(fileName);
    await file.save(fileContent);

    logger.info("File uploaded successfully.");
  } catch (error) {
    logger.error("Error uploading to GCP Bucket:", error);
    throw error;
  }
};

// Send email via Mailgun
const sendEmailViaMailgun = async (userEmail, emailStatus) => {
  logger.info(
    "Sending email via Mailgun to:",
    userEmail,
    "with status:",
    emailStatus
  );

  const mg = mailgun({
    apiKey: process.env.MAILGUN_API_KEY,
    domain: process.env.MAILGUN_DOMAIN,
  });

  const data = {
    from: "desai.kashi@northeastern.edu",
    to: userEmail,
    subject: "File Download Status",
    text: `Status of file download: ${emailStatus}`,
  };

  try {
    await mg.messages().send(data);
    logger.info("Email sent successfully.");
  } catch (error) {
    logger.error("Error sending email:", error);
    throw error;
  }
};

// Track emails in DynamoDB
const trackEmailsInDynamoDB = async (userEmail, dynamoTableName) => {
  logger.info(
    "Tracking email in DynamoDB table:",
    dynamoTableName,
    "for user:",
    userEmail
  );

  const dynamoDB = new AWS.DynamoDB();
  const params = {
    TableName: dynamoTableName,
    Item: {
      UserEmail: { S: userEmail },
      Timestamp: { N: `${Date.now()}` },
    },
  };

  try {
    await dynamoDB.putItem(params).promise();
    logger.info("Email tracked in DynamoDB.");
  } catch (error) {
    logger.error("Error tracking email in DynamoDB:", error);
    throw error;
  }
};
