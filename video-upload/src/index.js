/***
It orchestrates upload of videos to storage.
user UI -> gateway -> video-upload -> video-storage -> external cloud storage
                           |
                           -> RabbitMQ (uploaded message) -> metadata
***/
const express = require("express");
const mongodb = require("mongodb");
const amqp = require("amqplib");
const http = require("http");

/******
Globals
******/
//Create a new express instance.
const app = express();
const SVC_DNS_RABBITMQ = process.env.SVC_DNS_RABBITMQ;
const SVC_DNS_VIDEO_STORAGE = process.env.SVC_DNS_VIDEO_STORAGE;
const PORT = process.env.PORT && parseInt(process.env.PORT) || 3000;
const MAX_RETRIES = process.env.MAX_RETRIES && parseInt(process.env.MAX_RETRIES) || 10;
let READINESS_PROBE = false;

/***
Unlike most other programming languages or runtime environments, Node.js doesn't have a built-in
special "main" function to designate the entry point of a program.

Accessing the main module
-------------------------
When a file is run directly from Node.js, require.main is set to its module. That means that it is
possible to determine whether a file has been run directly by testing require.main === module.
***/
if (require.main === module)
{
  main()
  .then(() =>
  {
    READINESS_PROBE = true;
    console.log(`Microservice "video-upload" is listening on port "${PORT}"!`);
  })
  .catch(err =>
  {
    console.error('Microservice "video-upload" failed to start.');
    console.error(err && err.stack || err);
  });
}

function main()
{
  //Throw an exception if any required environment variables are missing.
  if (process.env.SVC_DNS_RABBITMQ === undefined)
  {
    throw new Error('Please specify the name of the service DNS for RabbitMQ in the environment variable SVC_DNS_RABBITMQ.');
  }
  else if (process.env.SVC_DNS_VIDEO_STORAGE === undefined)
  {
    throw new Error('Please specify the service DNS in the environment variable SVC_DNS_VIDEO_STORAGE.');
  }
  //Display a message if any optional environment variables are missing.
  else
  {
    if (process.env.PORT === undefined)
    {
      console.log('The environment variable PORT for the HTTP server is missing; using port 3000.');
    }
    //
    if (process.env.MAX_RETRIES === undefined)
    {
      console.log(`The environment variable MAX_RETRIES is missing; using MAX_RETRIES=${MAX_RETRIES}.`);
    }
  }
  //Notify when server has started.
  return requestWithRetry(connectToRabbitMQ, SVC_DNS_RABBITMQ, MAX_RETRIES)  //Connect to RabbitMQ...
  .then(channel =>                    //then...
  {
    return startHttpServer(channel);  //start the HTTP server.
  });
}

function connectToRabbitMQ(url, currentRetry)
{
  console.log(`Connecting (${currentRetry}) to 'RabbitMQ' at ${url}.`);
  /***
  return connect()
    .then(conn =>
    {
      //Create a RabbitMQ messaging channel.
      return conn.createChannel()
        .then(channel =>
        {
          //Assert that we have a "uploaded" exchange.
          return channel.assertExchange('uploaded', 'fanout')
            .then(() =>
            {
              return channel;
            });
        });
    });
  ***/
  return amqp.connect(url)
  .then(conn =>
  {
    console.log("Connected to RabbitMQ.");
    //Create a RabbitMQ messaging channel.
    return conn.createChannel()
    .then(channel =>
    {
      //Assert that we have a "viewed" queue.
      return channel.assertQueue('viewed', { exclusive: false })
      .then(() =>
      {
        return channel;
      });
    });
  });
}

async function sleep(timeout)
{
  return new Promise(resolve =>
  {
    setTimeout(() => { resolve(); }, timeout);
  });
}

async function requestWithRetry(func, url, maxRetry)
{
  for (let currentRetry = 0;;)
  {
    try
    {
      ++currentRetry;
      return await func(url, currentRetry);
    }
    catch(err)
    {
      if (currentRetry === maxRetry)
      {
        //Save the error from the most recent attempt.
        lastError = err;
        console.log(`Maximum number of ${maxRetry} retries has been reached.`);
        break;
      }
      const timeout = (Math.pow(2, currentRetry) - 1) * 100;
      console.log(`Waiting ${timeout}ms...`);
      await sleep(timeout);
    }
  }
  //Throw the error from the last attempt; let the error bubble up to the caller.
  throw lastError;
}

//Start the HTTP server.
function startHttpServer(channel)
{
  //Notify when the server has started.
  return new Promise(resolve =>
  {
    setupHandlers(channel);
    app.listen(PORT, () =>
    {
      //HTTP server is listening, resolve the promise.
      resolve();
    });
  });
}

//Setup event handlers.
function setupHandlers(channel)
{
  //Readiness probe.
  app.get('/readiness',
  (req, res) =>
  {
    res.sendStatus(READINESS_PROBE === true ? 200 : 500);
  });
  //
  //Route for uploading videos.
  app.post('/upload',
  (req, res) =>
  {
    const fileName = req.headers['file-name'];
    //Create a new unique ID for the video.
    const videoId = new mongodb.ObjectId() + `/${fileName}`;
    const newHeaders = Object.assign({}, req.headers, { id: videoId });
    streamToHttpPost(req, SVC_DNS_VIDEO_STORAGE, '/upload', newHeaders)
    .then(() =>
    {
      res.sendStatus(200);
    })
    .then(() =>
    {
      //sendMultipleRecipientMessage(channel, { id: videoId, name: fileName });
      sendSingleRecipientMessage(channel, { id: videoId, name: fileName });
    })
    .catch(err =>
    {
      console.error(`Failed to capture uploaded file ${fileName}.`);
      console.error(err);
      console.error(err.stack);
    });
  });
}

//A Node.js stream to a HTTP POST request.
function streamToHttpPost(inputStream, uploadHost, uploadRoute, headers)
{
  //Wait for it to complete.
  return new Promise((resolve, reject) =>
  {
    //Forward the request to the video storage microservice.
    const forwardRequest = http.request(
    {
      host: uploadHost,
      path: uploadRoute,
      method: 'POST',
      headers: headers
    });
    inputStream.on('error', reject);
    inputStream.pipe(forwardRequest)
    .on('error', reject)
    .on('end', resolve)
    .on('finish', resolve)
    .on('close', resolve);
  });
}

/***
function sendMultipleRecipientMessage(channel, videoMetadata)
{
  console.log('Publishing message on "uploaded" exchange.');
  const msg = { video: videoMetadata };
  const jsonMsg = JSON.stringify(msg);
  //Publish message to the "uploaded" exchange.
  channel.publish('uploaded', '', Buffer.from(jsonMsg));
}
***/

function sendSingleRecipientMessage(channel, videoMetadata)
{
  console.log('Publishing message on "uploaded" queue.');
  //Define the message payload. This is the data that will be sent with the message.
  const msg = { video: videoMetadata };
  //Convert the message to the JSON format.
  const jsonMsg = JSON.stringify(msg);
  //In RabbitMQ a message can never be sent directly to the queue, it always needs to go through an
  //exchange. Use the default exchange identified by an empty string; publish the message to the
  //"uploaded" queue.
  channel.publish('', 'uploaded', Buffer.from(jsonMsg));
}
