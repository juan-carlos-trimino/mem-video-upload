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
const winston = require('winston');
const { randomUUID } = require('crypto');

/******
Globals
******/
//Create a new express instance.
const app = express();
const SVC_NAME = process.env.SVC_NAME;
const APP_NAME_VER = process.env.APP_NAME_VER;
const SVC_DNS_RABBITMQ = process.env.SVC_DNS_RABBITMQ;
const SVC_DNS_VIDEO_STORAGE = process.env.SVC_DNS_VIDEO_STORAGE;
const PORT = process.env.PORT && parseInt(process.env.PORT) || 3000;
const MAX_RETRIES = process.env.MAX_RETRIES && parseInt(process.env.MAX_RETRIES) || 10;
let READINESS_PROBE = false;

/***
Resume Operation
----------------
The resume operation strategy intercepts unexpected errors and responds by allowing the process to
continue.
***/
process.on('uncaughtException',
err => {
  logger.error('Uncaught exception.', { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
})

/***
Abort and Restart
-----------------
***/
// process.on("uncaughtException",
// err => {
//   console.error("Uncaught exception:");
//   console.error(err && err.stack || err);
//   process.exit(1);
// })

//Winston requires at least one transport (location to save the log) to create a log.
const logConfiguration = {
  transports: [
    new winston.transports.Console()
  ],
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD hh:mm:ss.SSS'
    }),
    winston.format.json()
  ),
  exitOnError: false
}

//Create a logger and pass it the Winston configuration object.
const logger = winston.createLogger(logConfiguration);

/***
Unlike most other programming languages or runtime environments, Node.js doesn't have a built-in
special "main" function to designate the entry point of a program.

Accessing the main module
-------------------------
When a file is run directly from Node.js, require.main is set to its module. That means that it is
possible to determine whether a file has been run directly by testing require.main === module.
***/
if (require.main === module) {
  main()
  .then(() => {
    READINESS_PROBE = true;
    logger.info(`Microservice is listening on port ${PORT}!`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  })
  .catch(err => {
    logger.error('Microservice failed to start.', { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  });
}

function main() {
  //Throw an exception if any required environment variables are missing.
  if (process.env.SVC_DNS_RABBITMQ === undefined) {
    throw new Error('Please specify the name of the service DNS for RabbitMQ in the environment variable SVC_DNS_RABBITMQ.');
  }
  else if (process.env.SVC_DNS_VIDEO_STORAGE === undefined) {
    throw new Error('Please specify the service DNS in the environment variable SVC_DNS_VIDEO_STORAGE.');
  }
  //Display a message if any optional environment variables are missing.
  else {
    if (process.env.PORT === undefined) {
      logger.info(`The environment variable PORT for the HTTP server is missing; using port ${PORT}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    }
    //
    if (process.env.MAX_RETRIES === undefined) {
      logger.info(`The environment variable MAX_RETRIES is missing; using MAX_RETRIES=${MAX_RETRIES}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    }
  }
  //Notify when server has started.
  return requestWithRetry(connectToRabbitMQ, SVC_DNS_RABBITMQ, MAX_RETRIES)  //Connect to RabbitMQ...
  .then(channel => {                  //then...
    return startHttpServer(channel);  //start the HTTP server.
  });
}

/***
The user IP is determined by the following order:
 1. X-Client-IP
 2. X-Forwarded-For (Header may return multiple IP addresses in the format: "client IP, proxy1 IP, proxy2 IP", so take the the first one.)
    It's very easy to spoof:
    $ curl --header "X-Forwarded-For: 1.2.3.4" "http://localhost:3000"
 3. CF-Connecting-IP (Cloudflare)
 4. Fastly-Client-Ip (Fastly CDN and Firebase hosting header when forwared to a cloud function)
 5. True-Client-Ip (Akamai and Cloudflare)
 6. X-Real-IP (Nginx proxy/FastCGI)
 7. X-Cluster-Client-IP (Rackspace LB, Riverbed Stingray)
 8. X-Forwarded, Forwarded-For and Forwarded (Variations of #2)
 9. req.connection.remoteAddress
10. req.socket.remoteAddress
11. req.connection.socket.remoteAddress
12. req.info.remoteAddress
If an IP address cannot be found, it will return null.
***/
function getIP(req) {
  let ip = null;
  try {
    ip = req.headers['x-forwarded-for']?.split(',').shift() || req.socket?.remoteAddress || null;
    /***
    When the OS is listening with a hybrid IPv4-IPv6 socket, the socket converts an IPv4 address to
    IPv6 by embedding it within the IPv4-mapped IPv6 address format. This format just prefixes the
    IPv4 address with :ffff: (or ::ffff: for older mappings).
    Is the IP an IPv4 address mapped as an IPv6? If yes, extract the Ipv4.
    ***/
    const regex = /^:{1,2}(ffff)?:(?!0)(?!.*\.$)((1?\d?\d|25[0-5]|2[0-4]\d)(\.|$)){4}$/i;  //Ignore case.
    if (ip !== null && regex.test(ip)) {
      ip = ip.replace(/^.*:/, '');
    }
  }
  catch (err) {
    ip = null;
    logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
  }
  return ip;
}

function connectToRabbitMQ(url, currentRetry) {
  logger.info(`Connecting (${currentRetry}) to 'RabbitMQ' at ${url}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
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
  .then(conn => {
    logger.info(`Connected to 'RabbitMQ'.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
    //Create a RabbitMQ messaging channel.
    return conn.createChannel()
    .then(channel => {
      //Assert that we have a "viewed" queue.
      return channel.assertQueue('viewed', { exclusive: false })
      .then(() => {
        return channel;
      });
    });
  });
}

async function sleep(timeout) {
  return new Promise(resolve => {
    setTimeout(() => { resolve(); }, timeout);
  });
}

async function requestWithRetry(func, url, maxRetry) {
  for (let currentRetry = 0;;) {
    try {
      ++currentRetry;
      return await func(url, currentRetry);
    }
    catch(err) {
      if (currentRetry === maxRetry) {
        //Save the error from the most recent attempt.
        lastError = err;
        logger.info(`Maximum number of ${maxRetry} retries has been reached.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
        break;
      }
      const timeout = (Math.pow(2, currentRetry) - 1) * 100;
      logger.info(`Waiting ${timeout}ms...`, { app:APP_NAME_VER, service:SVC_NAME, requestId:'-1' });
      await sleep(timeout);
    }
  }
  //Throw the error from the last attempt; let the error bubble up to the caller.
  throw lastError;
}

//Start the HTTP server.
function startHttpServer(channel) {
  //Notify when the server has started.
  return new Promise(resolve => {
    setupHandlers(channel);
    app.listen(PORT, () => {
      //HTTP server is listening, resolve the promise.
      resolve();
    });
  });
}

//Setup event handlers.
function setupHandlers(channel) {
  //Readiness probe.
  app.get('/readiness',
  (req, res) => {
    res.sendStatus(READINESS_PROBE === true ? 200 : 500);
  });
  //
  //Route for uploading videos.
  app.post('/upload',
  (req, res) => {
    /***
    In the HTTP protocol, headers are case-insensitive; however, the Express framework converts
    everything to lower case. Unfortunately, for objects in JavaScript, their property names are
    case-sensitive.
    ***/
    const cid = req.headers['x-correlation-id'];
    const fileName = req.headers['file-name'];
    //Create a new unique ID for the video.
    const videoId = new mongodb.ObjectId() + `/${fileName}`;
    const newHeaders = Object.assign({}, req.headers, { id: videoId });
    logger.info(`Uploading ${videoId} to the video-storage microservice.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    streamToHttpPost(req, SVC_DNS_VIDEO_STORAGE, '/upload', newHeaders)
    .then(() => {
      logger.info(`Uploaded ${videoId} to the video-storage microservice.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      res.sendStatus(200);
    })
    .then(() => {
      //sendMultipleRecipientMessage(channel, { id: videoId, name: fileName });
      sendSingleRecipientMessage(channel, { id: videoId, name: fileName, cid: cid });
    })
    .catch(err => {
      logger.error(`Failed to upload the file ${fileName}.`, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      logger.error(err, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
      logger.error(err.stack, { app:APP_NAME_VER, service:SVC_NAME, requestId:cid });
    });
  });
}

//A Node.js stream to a HTTP POST request.
function streamToHttpPost(inputStream, uploadHost, uploadRoute, headers) {
  //Wait for it to complete.
  return new Promise((resolve, reject) => {
    //Forward the request to the video storage microservice.
    const forwardRequest = http.request({
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

function sendSingleRecipientMessage(channel, videoMetadata) {
  logger.info("Publishing message on 'uploaded' queue.", { app:APP_NAME_VER, service:SVC_NAME, requestId:videoMetadata.cid });
  //Define the message payload. This is the data that will be sent with the message.
  const msg = { video: videoMetadata };
  //Convert the message to the JSON format.
  const jsonMsg = JSON.stringify(msg);
  //In RabbitMQ a message can never be sent directly to the queue, it always needs to go through an
  //exchange. Use the default exchange identified by an empty string; publish the message to the
  //"uploaded" queue.
  channel.publish('', 'uploaded', Buffer.from(jsonMsg));
}
