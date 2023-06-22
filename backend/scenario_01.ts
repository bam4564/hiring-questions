/*
------------------------------------------------------------------------
Objective 
------------------------------------------------------------------------
This is an exercise where I will provide some context and some psuedocode and we'll work through 
multiple questions testing your knowledge of relevant technical concepts. 

The code provided is a modified version of part of the job definition executed by the worker microservice. 
The purpose of this job is to maintain a continuous daily price history for different tokens. 
This history is essential for performing historical value computations, such as calculating the 
cumulative value of rewards claimed by a user using prices at the point of claiming.

------------------------------------------------------------------------
Background (Architecture)
------------------------------------------------------------------------
Our backend system is implemented as a microservices architecture with the following services, each running in its own Docker container:
- Postgres Database
  - A single deployed instance. 
- Public API: The public facing API that services calls from the application frontend. 
  - Horizontally scalable as an ECS task group to N instances. 
- Admin API: Used for database updates, triggering jobs, and performing administrative operations. 
  - Has a single deployed instance. 
- Worker: Executes various background processing jobs such as monitoring events on the blockchain, updating historical price feeds for assets, and updating platform statistics. 
  - Horizontally scalable as an ECS task group to N instances. 

Our backend utilizes a simple publis / subscribe model (pgboss node.js library) for the job queue system, where:
- Handler functions are bound to job queues by name.
  // Sets up the queue subscription 
  pbgoss.subscribe('QUEUE_NAME', handlerFunction); 
  // Will create a job in this queue with this payload. 
  pgboss.send('QUEUE_NAME', { data: 'hello world' })  
  // Asynchronously, at some later point in time, the handler will be invoked 
  // within some worker with the payload passed as input
- Jobs remain in the queue until a worker retrieves them.
  - The queue system guarantees "Exactly-once" job delivery, ensuring a job is picked up only once. 
  However, multiple jobs of the same type within a single queue may result in different processes 
  executing the same code if they pick up jobs around the same time.
- Jobs can be scheduled for queues using cron schedules.
  // Sets up the scheduled job
  - pbgoss.schedule('QUEUE_NAME', '1 * * * * *', { data: 'hello world' }); 
- The queue system supports both singleton and non-singleton queues:
  - Singleton queues allow only a single job with a given key to be queued at once.
  - Non-singleton queues can accommodate multiple jobs simultaneously.

------------------------------------------------------------------------
Background (Token Price History Job)
------------------------------------------------------------------------
The handler function seen below can be triggered in two scenarios 

  1. There is some fan-out job that is triggered periodically via cron schedule. The purpose of this fan-out job 
  is to inspect the `TokenDailyPriceHistoryUsd` table and determine the max value of `timestamp` for each unique 
  `token`. We filter results based on whether or not the date of `timestamp` is less than the current day. This 
  signifies that we do not have fully up to date historical price data for some token. 

  select token, start_timestamp from (
    select token, max(timestamp) as start_timestamp
    from "TokenDailyPriceHistoryUsd"
    group by token 
  ) as _ 
  where date_trunc(current_date(), 'day') > date_trunc(start_timestamp, 'day')
  
  We take the result set from the above query and for each row, enqueue a job that will eventually 
  trigger the function `handler` with the corresponding input data. 
  - One note here, we also enqueue jobs for tokens with no prior data entries in "TokenDailyPriceHistoryUsd"
  
  2. The handler can be triggered via an API call to our admin API. This is something I do often to test 
  specific functionality within the backend. 
  
------------------------------------------------------------------------
Further Assumptions  
------------------------------------------------------------------------
- Assume that any price history stored within `TokenDailyPriceHistoryUsd` for a 
particular token is contiguous (i.e. no gaps between dates). 
- Assume there are 3 worker instances in the ECS worker task group. 
- Assume the job queue to which this handler is subscribed is non-singleton. 
*/ 

const handler = async ({ forceRefresh, token, startTimestamp }: { forceRefresh: boolean, token: string, startTimestamp: Date }) => {
  /*
  Here is a prisma schema (maps to a postgresql table) for the table referenced in this scenario: 
  model TokenDailyPriceHistoryUsd {
      id          Int    @id @default(autoincrement())
      token       String @db.VarChar(42)
      timestamp   DateTime
      price       Decimal
      @@unique([token, timestamp])
  }
  */ 

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
1. The conditional check performed in step 3 of the handler function above will be true in some cases. When might this occur? 
- Hints
  - This code is running within the worker serivice. Review the details of how this service operates. 
  - Think about the different ways this job handler can be triggered. 

2. There is some bug within this code that will cause an error to be thrown under specific circumstances 
(this will not occur on every run of this function). Please locate the bug and describe at a high level 
how you would fix it. 
- Hints:
  - This bug will arise under similar circumstances as question (1). 

3. In the assumptions outlined above, I stated that the job queue to which this handler function 
is subscribed is non-singleton. Do you think this is the right approach when considering the possibility 
of making this a singleton queue? Why or why not? 
*/