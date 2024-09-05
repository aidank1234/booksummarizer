import { S3, DynamoDB } from 'aws-sdk';
import axios from 'axios';

const s3 = new S3();
const dynamoDb = new DynamoDB.DocumentClient();

const MAX_TOKENS_PER_MINUTE = 30000;
const MAX_TOKENS_PER_REQUEST = 6000;

interface SummarizeChunksEvent {
  id: string;
  chunkS3Uri: string;
}

interface SummarySchema {
  chunkName: string;
  summary: string;
  keyQuotes: Array<{ character: string; quote: string }>;
}

interface OverarchingSummarySchema {
  themes: string[];
  characters: Array<{ character: string; description: string }>;
  synopsis: string;
  keyQuotes: Array<{ character: string; quote: string }>;
}

export const handler = async (event: SummarizeChunksEvent): Promise<{ status: string }> => {
  const { id, chunkS3Uri } = event;

  console.log(`Starting summarization for ID: ${id}, Chunk S3 URI: ${chunkS3Uri}`);

  // Fetch the chunks JSON from S3
  const chunks = await fetchChunksFromS3(chunkS3Uri);

  console.log(`Number of chunks to process: ${chunks.length}`);

  // Summarize each chunk in parallel with rate limiting
  const summaries = await summarizeChunksWithRateLimit(chunks);

  await new Promise(resolve => setTimeout(resolve, 15000));

  console.log("Awaiting 15 seconds prior to final summary generation");
  // Generate an overarching summary
  const overarchingSummary = await generateOverarchingSummary(summaries);

  // Combine all summaries into one final JSON object
  const finalSummary = {
    id,
    sections: summaries,
    overarchingSummary,
  };

  // Save the final summary to S3
  const outputKey = `summaries/${id}_final_summary.json`;
  const putParams = {
    Bucket: process.env.SUMMARY_BUCKET!,
    Key: outputKey,
    Body: JSON.stringify(finalSummary, null, 2),
    ContentType: 'application/json',
  };

  try {
    await s3.putObject(putParams).promise();
    console.log(`Final summary saved to S3 at key: ${outputKey}`);

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
    console.log(`DynamoDB updated with summary URL: ${s3Url}`);

    return { status: 'success' };
  } catch (error: any) {
    console.error(`Error saving summary for ID ${id}:`, error);
    throw new Error(`Error saving summary for ID ${id}: ${error.message}`);
  }
};

// Function to fetch the chunks from S3
async function fetchChunksFromS3(s3Uri: string): Promise<any> {
  const { Bucket, Key } = parseS3Uri(s3Uri);
  console.log(`Fetching chunks from S3 bucket: ${Bucket}, key: ${Key}`);

  const params = {
    Bucket,
    Key
  };

  const data = await s3.getObject(params).promise();
  console.log(`Chunks fetched from S3 for key: ${Key}`);
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
  console.log(`Parsed S3 URI: Bucket = ${Bucket}, Key = ${Key}`);

  return { Bucket, Key };
}

// Function to summarize each chunk using GPT-4o with structured JSON schema
async function summarizeChunksWithRateLimit(chunks: any[]): Promise<SummarySchema[]> {
  const summaries = [];
  let tokensUsed = 0;
  let startTime = Date.now();

  console.log(`Summarizing ${chunks.length} chunks with rate limiting...`);

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    const estimatedTokens = estimateTokens(chunk.chunkContent);
    console.log(`Processing chunk ${i + 1}/${chunks.length}, estimated tokens: ${estimatedTokens}`);

    // If sending this request will exceed the max tokens, wait
    if (tokensUsed + estimatedTokens > MAX_TOKENS_PER_MINUTE) {
      const elapsedTime = Date.now() - startTime;
      const remainingTime = 65000 - elapsedTime;

      console.log(`Rate limit exceeded, waiting for ${remainingTime} ms...`);

      // Ensure the delay only happens if we haven't waited for 65 seconds
      if (elapsedTime < 65000) {
        await new Promise((resolve) => setTimeout(resolve, remainingTime));
      }

      // Reset token usage and start time
      tokensUsed = 0;
      startTime = Date.now();
    }

    // Summarize the chunk using GPT-4o
    const summary = await summarizeChunk(chunk.chunkContent, chunk.chunkName);
    summaries.push(summary);

    // Update token usage
    tokensUsed += estimatedTokens;
    console.log(`Completed summary for chunk ${i + 1}/${chunks.length}, total tokens used: ${tokensUsed}`);
  }

  return summaries;
}

// Function to summarize a single chunk using GPT-4o's JSON output
async function summarizeChunk(chunkContent: string, chunkName: string): Promise<SummarySchema> {
  const prompt = `Summarize the following text in JSON format. Provide a comprehensive synopsis and up to 5 key quotes along with the character names. Response format:
  {
    "chunkName": "${chunkName}",
    "summary": "Comprehensive synopsis of the section.",
    "keyQuotes": [
      { "character": "Character name", "quote": "The character's quote" },
      { "character": "Character name", "quote": "Another quote" }
    ]
  }

  Text: ${chunkContent}`;

  try {
    console.log(`Sending request to OpenAI for chunk: ${chunkName}`);
    const response = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o-2024-08-06',
        messages: [
          { role: 'system', content: 'You are an assistant summarizing text in JSON format.' },
          { role: 'user', content: prompt }
        ],
        max_tokens: MAX_TOKENS_PER_REQUEST,
        temperature: 0.5,
        // No 'response_format', but request for a valid JSON response through the prompt
      },
      {
        headers: {
          'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );

    console.log(`Received response for chunk: ${chunkName}`);
    
    // Parse the response and return it as a JSON object
    return robustJsonParse(response.data.choices[0].message.content.trim());
  } catch (error: any) {
    // Log OpenAI API errors with detailed information
    if (error.response) {
      console.error('OpenAI API Error:', error.response.status); // HTTP status code
      console.error('OpenAI API Error Data:', JSON.stringify(error.response.data, null, 2)); // Full error response
    } else {
      console.error('Error:', error.message); // General error message
    }
    throw error;
  }
}

// Function to robustly parse JSON, handling AI's code block markers
function robustJsonParse(responseText: string): any {
  try {
    // Remove any surrounding code block markers (```json or ``` and ```)
    const cleanedText = responseText.replace(/```json|```/g, '').trim();
    
    return JSON.parse(cleanedText);
  } catch (error) {
    console.error('Failed to parse AI response as JSON. Response:', responseText);
    throw new Error('AI response was not valid JSON');
  }
}

// Function to estimate the number of tokens in a text chunk
function estimateTokens(text: string): number {
  const words = text.split(/\s+/).length;
  console.log(`Estimated tokens for text: ${Math.ceil(words * 1.33)}`);
  return Math.ceil(words * 1.33);
}

async function generateOverarchingSummary(summaries: any[]): Promise<OverarchingSummarySchema> {
  const combinedSummaries = summaries.map(s => s.summary).join('\n\n');

  // Prompt the model to return the overarching summary in JSON format
  const prompt = `Summarize the following sections into a final overarching summary in JSON format. Include key themes, characters, a brief synopsis, and key quotes. Response format:
  {
    "themes": ["theme1", "theme2"],
    "characters": [
      { "character": "Character name", "description": "Brief description of the character" }
    ],
    "synopsis": "Brief synopsis",
    "keyQuotes": [
      { "character": "Character name", "quote": "The character's quote" }
    ]
  }

  Sections: ${combinedSummaries}`;

  try {
    console.log('Sending request for overarching summary');
    const response = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model: 'gpt-4o-2024-08-06',
        messages: [
          { role: 'system', content: 'You are an assistant generating a final overarching summary in JSON format.' },
          { role: 'user', content: prompt }
        ],
        max_tokens: MAX_TOKENS_PER_REQUEST,
        temperature: 0.5,
        // No 'response_format', but request for a valid JSON response through the prompt
      },
      {
        headers: {
          'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );

    console.log('Received response for overarching summary');
    
    // Parse the response and return the parsed JSON object
    return robustJsonParse(response.data.choices[0].message.content.trim());
  } catch (error: any) {
    // Log OpenAI API errors with detailed information
    if (error.response) {
      console.error('OpenAI API Error:', error.response.status); // HTTP status code
      console.error('OpenAI API Error Data:', JSON.stringify(error.response.data, null, 2)); // Full error response
    } else {
      console.error('Error:', error.message); // General error message
    }
    throw error;
  }
}