const redis = require('redis');
const { connect, StringCodec } = require('nats');

// Define the Redis stream and consumer names
const stream = 'stock-data-stream';

// Create a Redis client with the correct connection settings
const client = redis.createClient({
  url: 'redis://:I3Linode@172.232.13.6:6379'
});

// Handle Redis connection errors
client.on('error', (err) => {
  console.error('Redis connection error:', err);
});

// Connect to the Redis server
client.connect().then(() => {
  console.log('Connected to Redis server');
}).catch(err => {
  console.error('Failed to connect to Redis server:', err);
});

// NATS configuration
const natsUrl = 'nats://host.docker.internal:4222';
const subject = 'redisprice2';
const sc = StringCodec();

// Function to publish messages to NATS
async function publishToNats(natsConnection, message) {
  try {
    natsConnection.publish(subject, sc.encode(JSON.stringify(message)));
    console.log(`Published message to NATS on subject "${subject}"`);
  } catch (err) {
    console.error('Failed to publish to NATS:', err);
  }
}

// Connect to NATS
async function connectToNats() {
  try {
    const natsConnection = await connect({ servers: natsUrl });
    console.log(`Connected to NATS at ${natsUrl}`);
    return natsConnection;
  } catch (err) {
    console.error('Error connecting to NATS:', err);
    throw err;
  }
}

// Subscribe to the Redis stream
async function subscribeToStream(natsConnection) {
  console.log(`Subscribed to Redis stream: ${stream}`);
  while (true) {
    try {
      const response = await client.xRead(
        { key: stream, id: '>' },
        { BLOCK: 5000 }
      );

      if (response) {
        response.forEach(streamData => {
          streamData.messages.forEach(message => {
            console.log('Received message:', message);

            // Publish the message to NATS
            publishToNats(natsConnection, message);
          });
        });
      }
    } catch (err) {
      console.error('Error reading from Redis stream:', err);
    }
  }
}

// Start the process
(async () => {
  try {
    const natsConnection = await connectToNats();
    await subscribeToStream(natsConnection);
  } catch (err) {
    console.error('Error in the subscription process:', err);
  }
})();
