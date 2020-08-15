require('dotenv').config();

const fs = require('fs');
const amqp = require('amqplib');
const redis = require('redis');
const asyncRedis = require("async-redis");
const parser = require('csv-parser');
const Minio = require('minio');
var minioClient = new Minio.Client({
    endPoint: process.env.BLOB_ENDPOINT,
    port: 9000,
    useSSL: false,
    accessKey: 'AKEY',
    secretKey: 'SKEY123456'
});
const MongoClient = require('mongodb').MongoClient;
const redisClient = redis.createClient(process.env.REDIS_URL);
const asyncRedisClient = asyncRedis.decorate(redisClient);

const { promisify } = require('util')
const sleep = promisify(setTimeout);

const CHUNK_SIZE = 50;

async function main() {
    try {
        const conn = await amqp.connect(process.env.QUEUE_URL);
        const ch = await conn.createChannel();
        const queueName = process.env.QUEUE_NAME;
        await ch.assertQueue(queueName);
        while (ch) {
            await ch.consume(queueName, async msg => {
                if (msg !== null) {
                    let jobDetails = JSON.parse(msg.content.toString());
                    console.log(jobDetails);
                    let jobId = jobDetails.job;
                    let statusKey = `${jobId}-STATUS`;
                    let progressKey = `${jobId}-PROGRESS`;
                    let status = await asyncRedisClient.get(statusKey);
                    if (status === 'COMPLETED') {
                        ch.ack(msg);
                        return;
                    }
                    let file = await asyncRedisClient.get(jobId);
                    minioClient.getObject('blob', file, async (err, stream) => {
                        let progress = await asyncRedisClient.get(progressKey);
                        progress = Number(progress);
                        console.log(`Progress: ${progress} ${progress}`);
                        let currentProgress = 0;
                        let results = [];
                        let dbClient = await MongoClient.connect(process.env.DB_URL);
                        let db = dbClient.db(process.env.DB_NAME);
                        let collection = db.collection(process.env.COLLECTION_NAME);
                        for await (const record of stream.pipe(parser())) {
                            currentProgress += 1;
                            if(currentProgress <= progress) continue;
                            results.push({...record, jobId: jobId});
                            if (currentProgress % CHUNK_SIZE == CHUNK_SIZE - 1) {
                                let insert = await collection.insertMany(results);
                                console.log(insert);
                                results = [];
                                await asyncRedisClient.set(progressKey, currentProgress.toString());
                                status = await asyncRedisClient.get(statusKey);
                                if(status !== 'RUNNING') {
                                    return;
                                }

                            }
                            await sleep(10);
                        }
                        await asyncRedisClient.set(statusKey, 'COMPLETED');
                        ch.ack(msg);
                    });
                }
            });
        }
    } catch (error) {
        console.error(error)
    }
}

main()
    .then()
    .catch(err => console.error(err));