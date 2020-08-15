require('dotenv').config();

const sh = require('short-hash');
const fs = require('fs');

const express = require('express');
const app = express();
const cors = require('cors');
const multer = require('multer');

const upload = multer({ dest: `files/` });
const SERVER_PORT = process.env.SERVER_PORT || 3000;

const amqp = require('amqplib');
const redis = require('redis');
const asyncRedis = require("async-redis");

const Minio = require('minio');
let minioClient = new Minio.Client({
    endPoint: process.env.BLOB_ENDPOINT,
    port: 9000,
    useSSL: false,
    accessKey: 'AKEY',
    secretKey: 'SKEY123456'
});
const MongoClient = require('mongodb').MongoClient;
const redisClient = redis.createClient(process.env.REDIS_URL);
const asyncRedisClient = asyncRedis.decorate(redisClient);

app.use(cors());

app.post('/job/start', upload.single('data'), async (req, res) => {
    let file = req.file.originalname;
    let jobId = generateJobId(req.file.filename);
    let statusKey = `${jobId}-STATUS`;
    minioClient.makeBucket('blob', 'us-east-1', (err) => {
        let filePath = req.file.path;
        let fileStream = fs.createReadStream(filePath);
        minioClient.putObject('blob', file, fileStream, async (err, tags) => {
            let message = {
                job: jobId,
                status: 'RUNNING'
            };
            await pushToJobQueue(message);
            await asyncRedisClient.set(jobId, file);
            await asyncRedisClient.set(statusKey, 'RUNNING');
            let progressKey = `${jobId}-PROGRESS`;
            await asyncRedisClient.set(progressKey, '0');
            return res.send({
                jobId: jobId,
                status: 'RUNNING'
            });
        })
    });
});

app.get('/job/:id/pause', async (req, res) => {
    let jobId = req.params.id;
    let statusKey = `${jobId}-STATUS`;
    let jobStatus = await asyncRedisClient.get(statusKey);
    if (jobStatus == null) {
        return res.send(`There is no job associated with the ID: ${jobId}`);
    }
    if (jobStatus !== 'RUNNING') {
        return res.send(`Only a 'RUNNING' job can be paused.`);
    }
    await asyncRedisClient.set(statusKey, 'PAUSED');
    res.send(`Job ${jobId} has been paused.`);
});

app.get('/job/:id/cancel', async (req, res) => {
    let jobId = req.params.id;
    let statusKey = `${jobId}-STATUS`;
    let jobStatus = await asyncRedisClient.get(statusKey);
    if (jobStatus == null) {
        return res.send(`There is no job associated with the ID: ${jobId}`);
    }
    if(jobStatus !== 'PAUSED') {
        return res.send('You need to pause a job before you cancel it.');
    }
    await asyncRedisClient.set(statusKey, 'CANCELED');
    let dbClient = await MongoClient.connect(process.env.DB_URL);
    let db = dbClient.db(process.env.DB_NAME);
    let collection = db.collection(process.env.COLLECTION_NAME);
    let delCount = collection.deleteMany({jobId: jobId});
    res.send(`Job ${jobId} has been canceled. Reverting your changes...`);    
});

app.get('/job/:id/resume', async (req, res) => {
    let jobId = req.params.id;
    let statusKey = `${jobId}-STATUS`;
    let jobStatus = await asyncRedisClient.get(statusKey);
    if (jobStatus == null) {
        return res.send(`There is no job associated with the ID: ${jobId}`);
    }
    if (jobStatus !== 'PAUSED') {
        return res.send(`Only paused jobs can be resumed.`);
    }
    let message = {
        job: jobId,
        status: 'RUNNING'
    };
    await pushToJobQueue(message);
    await asyncRedisClient.set(statusKey, 'RUNNING');
    res.send(`Job ${jobId} has resumed.`);
});


app.listen(SERVER_PORT, () => {
    console.log(`Server started at port ${SERVER_PORT}`);
});

const generateJobId = (file) => {
    return sh(file);
};

const pushToJobQueue = async (payload) => {
    const conn = await amqp.connect(process.env.QUEUE_URL);
    const ch = await conn.createChannel();
    const queueName = process.env.QUEUE_NAME;
    await ch.assertQueue(queueName);
    await ch.sendToQueue(queueName, Buffer.from(JSON.stringify(payload)));
}