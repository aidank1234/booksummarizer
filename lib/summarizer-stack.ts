import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as stepfunctions from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as path from 'path';

export class SummarizerStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Ensure OPENAI_API_KEY is defined
    const openaiApiKey = process.env.OPENAI_API_KEY;
    if (!openaiApiKey) {
      throw new Error('OPENAI_API_KEY environment variable is not defined');
    }

    // Create the DynamoDB table
    const summarizerTable = new dynamodb.Table(this, 'SummarizerTable', {
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
      tableName: 'SummarizerTable',
      removalPolicy: cdk.RemovalPolicy.DESTROY,  // Only for testing, change for production
    });

    // Create the S3 bucket for storing the full text content of works (input files)
    const inputBucket = new s3.Bucket(this, 'InputBucket', {
      bucketName: 'bookssummarizer-full-text-bucket',
      removalPolicy: cdk.RemovalPolicy.DESTROY,  // Only for testing, change for production
    });

    // Create the S3 bucket for storing JSON chunks of the works (private access)
    const outputBucket = new s3.Bucket(this, 'OutputBucket', {
      bucketName: 'bookssummarizer-json-chunks-bucket',
      removalPolicy: cdk.RemovalPolicy.DESTROY,  // Only for testing, change for production
    });

    // Create the S3 bucket for storing the final summarized JSON outputs (public access)
    const summaryBucket = new s3.Bucket(this, 'SummaryBucket', {
      bucketName: 'bookssummarizer-summary-bucket',
      publicReadAccess: true,  // Enable public access for this bucket
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ACLS,  // Allow public access
      removalPolicy: cdk.RemovalPolicy.DESTROY,  // Only for testing, change for production
    });

    // Environment variables for the Lambda functions
    const environmentVariables = {
      TABLE_NAME: summarizerTable.tableName,
      OUTPUT_BUCKET: outputBucket.bucketName,
      SUMMARY_BUCKET: summaryBucket.bucketName,
      OPENAI_API_KEY: openaiApiKey  // Ensure the API key is a string
    };

    // Lambda function to get file from DynamoDB and S3
    const getFileLambda = new lambda.Function(this, 'GetFileLambda', {
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/get-file')),
      environment: environmentVariables,
      timeout: cdk.Duration.seconds(120),
    });

    // Grant the Lambda function read access to the DynamoDB table
    summarizerTable.grantReadData(getFileLambda);

    // Grant the Lambda function read access to the input S3 bucket
    inputBucket.grantRead(getFileLambda);

    // Grant the Lambda function write access to the output S3 bucket
    outputBucket.grantWrite(getFileLambda);

    // Lambda function for Smart Chunking
    const smartChunkLambda = new lambda.Function(this, 'SmartChunkLambda', {
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/smart-chunk')),
      environment: environmentVariables,
      timeout: cdk.Duration.seconds(400),
    });

    // Grant the Lambda function write access to the output S3 bucket
    outputBucket.grantWrite(smartChunkLambda);

    // Grant the Lambda function read access to the output S3 bucket (for fetching chunks)
    outputBucket.grantRead(smartChunkLambda);

    // Lambda function for AI-based summarization (after chunking)
    const aiSummarizationLambda = new lambda.Function(this, 'AISummarizationLambda', {
      runtime: lambda.Runtime.NODEJS_18_X,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/ai-summarization')),
      environment: environmentVariables,
      timeout: cdk.Duration.seconds(900),  // Adjust based on expected completion time
    });

    // Grant the Lambda function read access to the output S3 bucket (for fetching chunked data)
    outputBucket.grantRead(aiSummarizationLambda);

    // Grant the Lambda function write access to the summary S3 bucket (for storing the final summary)
    summaryBucket.grantWrite(aiSummarizationLambda);

    // Grant the Lambda function write access to the DynamoDB table (for updating output URL)
    summarizerTable.grantWriteData(aiSummarizationLambda);

    // Define the Step Function tasks
    const getFileTask = new tasks.LambdaInvoke(this, 'Get File Task', {
      lambdaFunction: getFileLambda,
      outputPath: '$.Payload'
    });

    const smartChunkTask = new tasks.LambdaInvoke(this, 'Smart Chunk Task', {
      lambdaFunction: smartChunkLambda,
      outputPath: '$.Payload'
    });

    const aiSummarizationTask = new tasks.LambdaInvoke(this, 'AI Summarization Task', {
      lambdaFunction: aiSummarizationLambda,
      outputPath: '$.Payload'
    });

    // Define the Step Function workflow
    const definition = getFileTask
      .next(smartChunkTask)
      .next(aiSummarizationTask);

    const summarizerStateMachine = new stepfunctions.StateMachine(this, 'SummarizerStateMachine', {
      definition,
      timeout: cdk.Duration.minutes(10)  // Adjust as needed
    });

    // Outputs
    new cdk.CfnOutput(this, 'StateMachineARN', {
      value: summarizerStateMachine.stateMachineArn
    });
  }
}