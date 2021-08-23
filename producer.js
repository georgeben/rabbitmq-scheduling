const amqp = require("amqplib");
const dayjs = require("dayjs");

const DELAY_EXCHANGE = "my-delay-exchange";
const routingKey = "myapp.message.post";
const queueName = "message-queue";

// Connects to RabbitMQ
async function connectToRabbitMQ() {
  const conn = await amqp.connect(process.env.RABBITMQ_CONN_STRING);
  const channel = await conn.createConfirmChannel();
  return {
    conn,
    channel,
  };
}

// Posts a tweet to the console
function postTweet(message) {
  console.log(`🐦: ${message}`);
}

// listen for tweets that have been scheduled
async function handleScheduledTweets() {
  console.log("Waiting for scheduled tweets");
  const { conn, channel } = await connectToRabbitMQ();

  // Declare the delay exchange
  await channel.assertExchange(DELAY_EXCHANGE, "x-delayed-message", {
    autoDelete: false,
    durable: true,
    passive: true,
    arguments: {
      "x-delayed-type": "direct",
    },
  });

  // Creates a queue if it doesn't already exist
  const q = await channel.assertQueue(queueName, { durable: true });
  // Bind the queue to the delay exchange. When the the duration of the delay is over, the delay exchange will
  // route the message to this queue for processing
  await channel.bindQueue(q.queue, DELAY_EXCHANGE, routingKey);

  // Get a message from the queue that is ready for processing
  await channel.prefetch(1);

  await channel.consume(q.queue, (msg) => {
    const message = JSON.parse(msg.content.toString());
    console.log("Received a scheduled tweet");
    postTweet(message.tweet);
    channel.ack(msg);
  });
}

async function run() {
  console.log(process.argv[0])
  const tweet = process.argv[2]; // Tweet stores the third argument passed from the terminal
  const time = process.argv[3]; // Time stores the fourth argument passed from the terminal

  if (!tweet) {
    console.log("Please specify a tweet");
    process.exit(); // Terminates the app
  }

  if (!time) {
    // If no time is set, post tweet immediately
    return postTweet(tweet);
  }

  const scheduledTime = dayjs(time);
  // Validate the time specified by the user
  if (!scheduledTime.isValid()) {
    console.log("Invalid date specified");
    process.exit();
  }

  // Ensure that the time specified is greater than the current time
  if (dayjs().isAfter(scheduledTime)) {
    console.log("Please enter a date greater that 'now'");
    process.exit();
  }

  try {
    const { conn, channel } = await connectToRabbitMQ();
    await channel.assertExchange(DELAY_EXCHANGE, "x-delayed-message", {
      autoDelete: false,
      durable: true,
      passive: true,
      arguments: {
        "x-delayed-type": "direct",
      },
    });
    const q = await channel.assertQueue(queueName, { durable: true });
    await channel.bindQueue(q.queue, DELAY_EXCHANGE, routingKey);

    // Create the content of the message to be sent to RabbitMQ
    const message = Buffer.from(
      JSON.stringify({
        tweet,
      }),
    );

    // Calculate the delay for the message
    const delayInMilliseconds = scheduledTime.subtract(dayjs()).valueOf();

    // Publish the message to the delay exchange to hold for delayInMilliseconds
    await channel.publish(DELAY_EXCHANGE, routingKey, message, {
      deliveryMode: 2,
      mandatory: true,
      headers: {
        "x-delay": delayInMilliseconds,
      },
    });
    console.log("Published message to queue");
  } catch (error) {
    // Handle error
    console.log("Oops! An error occurred", error);
  }
}

run();
handleScheduledTweets();
