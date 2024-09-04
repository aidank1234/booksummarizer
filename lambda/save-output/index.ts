import { S3, DynamoDB } from 'aws-sdk';

const s3 = new S3();
const dynamoDb = new DynamoDB.DocumentClient();

interface SaveOutputEvent {
  id: string;
  chunkS3Uri: string;
}

export const handler = async (event: SaveOutputEvent): Promise<{ status: string }> => {
  const { id, chunkS3Uri } = event;

  // Fetch the chunks JSON from S3
  const chunks = await fetchChunksFromS3(chunkS3Uri);

  // Save the chunks to S3 as the final JSON
  const outputKey = `summaries/${id}.json`;
  const putParams = {
    Bucket: process.env.SUMMARY_BUCKET!,
    Key: outputKey,
    Body: JSON.stringify(chunks, null, 2),
    ContentType: 'application/json',
  };

  try {
    await s3.putObject(putParams).promise();

    // Construct the S3 URL
    const s3Url = `https://${process.env.SUMMARY_BUCKET!}.s3.amazonaws.com/${outputKey}`;

    // Update the DynamoDB table with the S3 URL
    const updateParams = {
      TableName: process.env.TABLE_NAME!,
      Key: { id },
      UpdateExpression: 'set #outputUrl = :outputUrl',
      ExpressionAttributeNames: {
        '#outputUrl': 'outputUrl'
      },
      ExpressionAttributeValues: {
        ':outputUrl': s3Url
      }
    };

    await dynamoDb.update(updateParams).promise();

    return { status: 'success' };
  } catch (error: any) {
    console.error(`Error saving output for ID ${id}:`, error);
    throw new Error(`Error saving output for ID ${id}: ${error.message}`);
  }
};

// Function to fetch the chunks from S3
async function fetchChunksFromS3(s3Uri: string): Promise<any> {
  const { Bucket, Key } = parseS3Uri(s3Uri);

  const params = {
    Bucket,
    Key
  };

  const data = await s3.getObject(params).promise();
  return JSON.parse(data.Body!.toString('utf-8'));
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