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

// Smart Chunking Logic with case-insensitive "Chapter" detection and sentence grouping
function smartChunkText(text: string): Chunk[] {
  console.log('Starting text chunking process');
  const chunkSize = 100000; // Max chunk size in characters (~20,000 words)
  const chunks: Chunk[] = [];

  // Updated pattern to match "Chapter" case-insensitive
  const chapterPattern = /\bchapter\b/gi;
  const matches = Array.from(text.matchAll(chapterPattern));

  let chapterNumber = 0;
  let sectionNumber = 1;
  let lastIndex = 0;

  if (matches.length > 0) {
    console.log(`Pattern "Chapter" found, number of matches: ${matches.length}`);

    // Handle any prefix text before the first chapter
    if (matches[0].index! > 0) {
      const prefixContent = text.slice(0, matches[0].index).trim();
      if (prefixContent) {
        chunks.push({ chunkName: `Prefix 1.1`, chunkContent: prefixContent });
        console.log(`Created chunk: Prefix 1.1, length: ${prefixContent.length}`);
      }
    }

    for (const [i, match] of matches.entries()) {
      chapterNumber++;
      sectionNumber = 1;

      if (match.index !== undefined && match.index > lastIndex) {
        // Create a chunk for the text before the current chapter
        if (lastIndex > 0) {
          const chunkContent = text.slice(lastIndex, match.index).trim();
          if (chunkContent) {
            chunks.push({
              chunkName: `Chapter ${chapterNumber - 1}.${sectionNumber}`,
              chunkContent
            });
            console.log(`Created chunk: Chapter ${chapterNumber - 1}.${sectionNumber}, length: ${chunkContent.length}`);
            sectionNumber++;
          }
        }

        lastIndex = match.index;

        // Handle the current chapter text
        const nextIndex = matches[i + 1]?.index ?? text.length;
        const chapterContent = text.slice(match.index, nextIndex).trim();

        chunks.push({ chunkName: `Chapter ${chapterNumber}.1`, chunkContent: chapterContent });
        console.log(`Created chunk: Chapter ${chapterNumber}.1, length: ${chapterContent.length}`);

        lastIndex = nextIndex;
      }
    }

    // Add any remaining text as the last chunk (if it exists)
    if (lastIndex < text.length) {
      const chunkContent = text.slice(lastIndex).trim();
      if (chunkContent) {
        chunks.push({ chunkName: `Suffix ${chapterNumber + 1}.1`, chunkContent: chunkContent });
        console.log(`Added final chunk: Suffix ${chapterNumber + 1}.1, length: ${chunkContent.length}`);
      }
    }
  } else {
    // If no pattern is found, treat the entire text as one section
    chunks.push({ chunkName: 'Section 1.1', chunkContent: text });
    console.log('No patterns found, treating entire text as one chunk');
  }

  // Fallback: Split large chunks into smaller chunks based on grouped sentences
  const finalChunks: Chunk[] = [];
  for (const chunk of chunks) {
    if (chunk.chunkContent.length > chunkSize) {
      console.log(`Chunk too large, splitting: ${chunk.chunkName}`);
      finalChunks.push(...fallbackChunking(chunk.chunkContent, chunk.chunkName, chunkSize));
    } else {
      finalChunks.push(chunk);
    }
  }

  console.log('Text chunking process completed');
  return finalChunks;
}

// Fallback function to split large chunks into smaller chunks based on grouped sentences
function fallbackChunking(text: string, baseName: string, maxChunkSize: number): Chunk[] {
  const chunks: Chunk[] = [];
  let currentChunk = '';
  let sectionNumber = 1;
  let sentenceGroup: string[] = [];

  // Split by sentences
  const sentences = text.match(/[^.!?]*[.!?]/g) || [text]; // Split by sentences
  
  for (const sentence of sentences) {
    sentenceGroup.push(sentence);
    if (sentenceGroup.length >= 4 || (currentChunk.length + sentence.length) > maxChunkSize) {
      const groupContent = sentenceGroup.join(' ');
      if ((currentChunk.length + groupContent.length) > maxChunkSize) {
        // Save the current chunk
        chunks.push({
          chunkName: `${baseName}.${sectionNumber}`,
          chunkContent: currentChunk.trim()
        });
        console.log(`Created fallback chunk: ${baseName}.${sectionNumber}, length: ${currentChunk.length}`);
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
      chunkName: `${baseName}.${sectionNumber}`,
      chunkContent: currentChunk.trim()
    });
    console.log(`Added final fallback chunk: ${baseName}.${sectionNumber}, length: ${currentChunk.length}`);
  }

  return chunks;
}