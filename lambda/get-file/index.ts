import { DynamoDB } from 'aws-sdk';
import { S3 } from 'aws-sdk';
import { APIGatewayProxyResult } from 'aws-lambda';
import { v4 as uuidv4 } from 'uuid';  // For generating unique keys in S3
const pdfParse = require('pdf-parse');

const dynamoDb = new DynamoDB.DocumentClient();
const s3 = new S3();

interface GetFileEvent {
  id: string;
}

export const handler = async (event: GetFileEvent): Promise<{ id: string; s3Uri: string } | APIGatewayProxyResult> => {
  const { id } = event;

  const params = {
    TableName: process.env.TABLE_NAME!,
    Key: { id }
  };

  try {
    const data = await dynamoDb.get(params).promise();

    if (!data.Item || !data.Item.s3Uri || !data.Item.fileName) {
      console.error(`File not found in DynamoDB for ID: ${id}`);
      return {
        statusCode: 404,
        body: `File not found in DynamoDB for ID: ${id}`
      };
    }

    const s3Uri = data.Item.s3Uri;
    const fileName = data.Item.fileName;

    // Fetch the file from S3 using the S3 URI
    const fileBuffer = await fetchFileFromS3(s3Uri);

    let textContent: string;

    if (fileName.endsWith('.pdf')) {
      textContent = await extractTextFromPdf(fileBuffer);
    } else if (fileName.endsWith('.txt')) {
      textContent = fileBuffer.toString('utf-8');
    } else {
      console.error(`Unsupported file type for ID: ${id}`);
      return {
        statusCode: 400,
        body: `Unsupported file type for ID: ${id}`
      };
    }

    // Save the extracted text content to S3
    const outputKey = `outputs/${uuidv4()}.txt`;  // Generate a unique key for the output file
    const putParams = {
      Bucket: process.env.OUTPUT_BUCKET!,
      Key: outputKey,
      Body: textContent,
      ContentType: 'text/plain'
    };
    await s3.putObject(putParams).promise();

    // Return the id and the S3 URI of the saved text content
    return { id, s3Uri: `s3://${process.env.OUTPUT_BUCKET}/${outputKey}` };

  } catch (error: any) {
    console.error(`Error processing DynamoDB item for ID: ${id}`, error);
    return {
      statusCode: 500,
      body: `Error processing DynamoDB item for ID: ${id}, ${error.message}`
    };
  }
};

// Function to fetch the file from S3 using the S3 URI
async function fetchFileFromS3(s3Uri: string): Promise<Buffer> {
  const { Bucket, Key } = parseS3Uri(s3Uri);

  const params = {
    Bucket,
    Key
  };

  const data = await s3.getObject(params).promise();
  return data.Body as Buffer;
}

// Function to parse the S3 URI and extract the bucket name and key
function parseS3Uri(s3Uri: string): { Bucket: string; Key: string } {
  if (!s3Uri.startsWith('s3://')) {
    throw new Error(`Invalid S3 URI: ${s3Uri}`);
  }

  const uriParts = s3Uri.slice(5).split('/');
  const Bucket = uriParts.shift()!;
  const Key = uriParts.join('/');

  return { Bucket, Key };
}

// PDF to text conversion using pdf-parse
async function extractTextFromPdf(pdfBuffer: Buffer): Promise<string> {
  const data = await pdfParse(pdfBuffer);
  return data.text;
}