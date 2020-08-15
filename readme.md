## Running

1. docker run --rm -p 5672:5672 --name ribbit -d rabbitmq
2. docker run --rm -p 27017:27017 --name mango -d mongo
3. docker run --rm -p 6379:6379 --name raddish -d redis
4. docker run --rm -d -p 9000:9000  --name gareebs3 -e "MINIO_ACCESS_KEY=AKEY"   -e "MINIO_SECRET_KEY=SKEY123456"   minio/minio server /data
5. docker run --rm -p 8081:8081 --name explorer --link mango:mongo -d mongo-express
6. cd api/ && npm i && npm start (in another terminal!)
7. cd worker/ && npm i && npm start (in another terminal!)

## Testing

`curl --location --request POST 'localhost:3000/job/start' --form 'data=@./testdata.csv'`

Take note of the job id

`curl localhost:3000/job/{jobId}/pause`

`curl localhost:3000/job/{jobId}/resume`

`curl localhost:3000/job/{jobId}/cancel`

Go to localhost:8081 and select `db` to monitor the changes
