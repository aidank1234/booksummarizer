import { S3 } from 'aws-sdk';
import { v4 as uuidv4 } from 'uuid';

const s3 = new S3();

interface SmartChunkEvent {
  id: string;
  s3Uri: string;
}

interface Chunk {
  chunkName: string;
  chunkContent: string;
}

export const handler = async (event: SmartChunkEvent): Promise<{ id: string; chunkS3Uri: string }> => {
  const { id, s3Uri } = event;

  console.log(`Starting smart chunking for ID: ${id}`);
  console.log(`Fetching text from S3 URI: ${s3Uri}`);

  // Fetch the text content from S3
  const textContent = await fetchTextFromS3(s3Uri);
  console.log(`Fetched text content for ID: ${id}, length: ${textContent.length}`);

  // Call the smartChunkText function to process the text into chunks
  const chunks = smartChunkText(textContent);
  console.log(`Smart chunking completed for ID: ${id}, number of chunks: ${chunks.length}`);

  // Save the chunks to S3 (without public access)
  const chunkKey = `chunks/${id}_${uuidv4()}.json`;
  console.log(`Saving chunks to S3 with key: ${chunkKey}`);
  const putParams = {
    Bucket: process.env.OUTPUT_BUCKET!,
    Key: chunkKey,
    Body: JSON.stringify(chunks, null, 2),
    ContentType: 'application/json',
    ACL: 'private'  // Make the object private
  };

  await s3.putObject(putParams).promise();
  console.log(`Chunks saved to S3 for ID: ${id}`);

  const chunkS3Uri = `s3://${process.env.OUTPUT_BUCKET!}/${chunkKey}`;
  console.log(`Returning chunk S3 URI: ${chunkS3Uri}`);

  // Return the S3 URI of the saved chunks
  return { id, chunkS3Uri };
};

// Function to fetch the text content from S3
async function fetchTextFromS3(s3Uri: string): Promise<string> {
  const { Bucket, Key } = parseS3Uri(s3Uri);
  console.log(`Fetching object from S3 bucket: ${Bucket}, key: ${Key}`);

  const params = {
    Bucket,
    Key
  };

  const data = await s3.getObject(params).promise();
  console.log(`Fetched object from S3`);
  return data.Body!.toString('utf-8');
}

// Function to parse the S3 URI and extract the bucket name and key
function parseS3Uri(s3Uri: string): { Bucket: string; Key: string } {
  if (!s3Uri.startsWith('s3://')) {
    throw new Error(`Invalid S3 URI: ${s3Uri}`);
  }

  const uriParts = s3Uri.slice(5).split('/');
  const Bucket = uriParts.shift()!;
  const Key = uriParts.join('/');
  console.log(`Parsed S3 URI: Bucket = ${Bucket}, Key = ${Key}`);

  return { Bucket, Key };
}

// Smart Chunking Logic with flat section numbering
function smartChunkText(text: string): Chunk[] {
  console.log('Starting text chunking process');
  const chunkSize = 40000; // Target chunk size in characters (~10,000 words)
  const chunks: Chunk[] = [];

  // Split by sentences
  const sentences = text.match(/[^.!?]*[.!?]/g) || [text];
  let currentChunk = '';
  let sectionNumber = 1;
  let sentenceGroup: string[] = [];

  for (const sentence of sentences) {
    sentenceGroup.push(sentence);
    if (sentenceGroup.length >= 4 || (currentChunk.length + sentence.length) > chunkSize) {
      const groupContent = sentenceGroup.join(' ');
      if ((currentChunk.length + groupContent.length) > chunkSize) {
        // Save the current chunk
        chunks.push({
          chunkName: `Section ${sectionNumber}`,
          chunkContent: currentChunk.trim()
        });
        console.log(`Created chunk: Section ${sectionNumber}, length: ${currentChunk.length}`);
        sectionNumber++;
        currentChunk = '';  // Start a new chunk
      }
      currentChunk += groupContent + ' ';
      sentenceGroup = [];
    }
  }

  // Add any remaining content as the last chunk
  if (currentChunk.length > 0) {
    chunks.push({
      chunkName: `Section ${sectionNumber}`,
      chunkContent: currentChunk.trim()
    });
    console.log(`Added final chunk: Section ${sectionNumber}, length: ${currentChunk.length}`);
  }

  console.log('Text chunking process completed');
  return chunks;
}