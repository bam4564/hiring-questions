/*

Background  
------------------------------------------------------------------------
This is some code that I took from the backend and modified a bit to simplify for the 
purposes of using it as an example. Here is some useful context 

- Our backend is implemented as a microservices architecture consisting of the following services. 
Each service is deployed in its own docker container. 
  - Postgres Database
    - Has a single deployed instance. 
  - Public API: The public facing API that services calls from the deployed application frontend. 
    - Horizontally scalable as an ECS task group to N instances. For now we just use 1 in production. 
  - Admin API: The admin facing API that I leverage to update the database, trigger jobs, and do other admin ops. 
    - Has a single deployed instance. 
  - Worker: A generic container that executes a variety of different background processing job like monitoring for events on chain, updating historical price feeds for assets, updating historical data on platform statistics, etc.
    - Horizontally scalable as an ECS task group to N instances. 

The code that you see below is one of the Job definitions that are executed on some schedule (or in response to external 
triggers) within the worker microservice. The goal of this particular job is to maintain a contiguous price ($ denominated) 
history at a daily cadence for a variety of tokens. This is useful for performing historical value computations (ex: what 
is the cumulative value of all rewards claimed by a user, where we use prices at the point of claiming rather than price 
at the point this data is requested). 

Our job queue system on the backend is a simple publish / subscribe model where 
- Handler functions are bound to job queues by name 
- When a job is pushed to a queue with a particular name, it will sit in the queue until 
it is retrieved by one of the workers. Once a worker takes a job off the queue, it will 
execute the subscribed handler function with an input payload optionally provided.
- We can schedule jobs to be pushed to queues using cron schedules. 
- Our queue system supports singleton and non-singleton queues. 
  - Singleton queues allow only a single job with a given key to be queued at once (two subsequent requests to enqueue a job with the same key will result in only a single job being queued). 
  - Non-singleton queues, where any number of jobs can be queued at a time. 
- Our queue system provides the guarantee of "Exactly-once" job delivery, meaning that an enqueued job 
is guaranteed to be only picked up once. There will never be two processes that take on the same job. 
Note if multiple jobs of the same type exist in a single queue, it is still possible for two processes
to be executing the same code if they pick up the two jobs around the same time.

Here is a prisma schema (maps to a postgresql table) for the table referenced in this scenario: 
model TokenDailyPriceHistoryUsd {
    id          Int    @id @default(autoincrement())
    token       String @db.VarChar(42)
    timestamp   DateTime
    price       Decimal
    @@unique([token, timestamp])
}

Information 
------------------------------------------------------------------------
When and how is this function `handler` triggered? 

  1. There is some fan-out job that is triggered periodically via cron schedule. The purpose of this fan-out job 
  is to inspect the `TokenDailyPriceHistoryUsd` table and determine the max value of `timestamp` for each unique 
  `token`.

  select token, max(timestamp) as timestamp
  from "TokenDailyPriceHistoryUsd"
  group by token 

  For each computed tuple (`token`, `timestamp`), we enqueue a job that will eventually trigger the function 
  `handler` with the tuple as input if and only if the date `timestamp` is less than the current 
  date (i.e. `timestamp` = 06/22/2023 and current date is 06/23/2023). We ignore the time component and 
  are only interested in the date. 

  2. Additionally, this handler can be triggered via an authenticated call to our admin API. This API 
  supports an endpoint that allows for enqueuing payloads into a job queue, so it can be used to trigger 
  any kind of backend job that we support. 

You may assume that any price history stored within `TokenDailyPriceHistoryUsd` for a particular 
token is contiguous (i.e. there is data for all days between some start and end date, with no gaps). 

Assume there are 3 worker instances in the ECS worker task group. 

Assume the job queue to which this handler is subscribed is non-singleton. 
*/ 

const handler = async ({ forceRefresh, token, startTimestamp }: { forceRefresh: boolean, token: string, startTimestamp: Date }) => {

  // 1. Compute the set of prices to append to the `TokenDailyPriceHistoryUsd` table 
  const prices = await apiCallToGetPrices(token, startTimestamp, 'daily'); 

  // 2. Get the max timestamp in the `TokenDailyPriceHistoryUsd` table for the target token 
  const {
    _max: { timestamp: maxTimestamp },
  } = await this.prisma.tokenDailyPriceHistoryUsd.aggregate({
    _max: { timestamp: true },
    where: { token },
  });

  // 3. Validate that the maxTimestamp plus one day is equal to the min price point
  const nextTimestampEpoch: number = prices[0].timestamp; // assume `prices` has at least one element. This value is a unix epoch timestamp.
  if (nextTimestampEpoch !== unixEpoch(addOneDay(maxTimestamp))) {
    return;
  }

  // 4. Perform deletes / creates, given that our data inputs are valid.
  const deletedCount = forceRefresh ? (await this.prisma.tokenDailyPriceHistoryUsd.deleteMany({ where: { token } })).count : 0;
  const createdCount = (
    await this.prisma.tokenDailyPriceHistoryUsd.createMany({
      data: prices.map(({ price, timestamp }) => ({
        timestamp: new Date(timestamp * 1000),
        price,
        token,
      })),
    })
  ).count;

}

/*
Questions  
------------------------------------------------------------------------
1. It is possible for the condition on line 77 to be true in some cases. What might these cases be? 
- Hints
  - This code is running within the worker serivice. Review the details of how this service operates. 
  - Think about the different ways this job handler can be triggered. 

2. I intentionally introduced a bug into the above code, please find it. 
- Hints:
  - This bug will arise under similar circumstances as question (1). 

3. In the assumptions outlined above, I stated that the job queue to which this handler function 
is subscribed is non-singleton. Do you think this is the right approach when considering the possibility 
of making this a singleton queue? Why or why not? 


*/