/*
This is some code that I took from the backend and modified a bit to simplify for the 
purposes of using it as an example. Here is some useful context 

- Our backend leverages a microservices architecture consisting of the following services 
  - Public API: The public facing API that services calls from the deployed application frontend. 
    - Has one or more deployed instances. 
  - Admin API: The admin facing API that I leverage to update the database, trigger jobs, and do other admin ops. 
    - Has a single deployed instance. 
  - Worker: A generic container that executes a variety of different background processing jobs like 
    - Monitoring for events on chain 
    - Routinely updating computed statistics about the platform (# stakers, staked balances by account and vault, )










Database schema:

model TokenDailyPriceHistoryUsd {
    id          Int    @id @default(autoincrement())
    token       String @db.VarChar(42)
    timestamp   DateTime
    price       Decimal
    @@unique([token, timestamp])
}
*/ 

// 1. Compute the set of prices to append to the `TokenDailyPriceHistoryUsd` table 
const token = '0x...'; 
const startTimestamp = new Date(...); 
const prices = await apiCallToGetPricesUsd(token, startTimestamp, 'daily'); 

// 2. Get the max timestamp in the `TokenDailyPriceHistoryUsd` table for the target token 
const {
  _max: { timestamp: maxTimestamp },
} = await this.prisma.tokenDailyPriceHistoryUsd.aggregate({
  _max: { timestamp: true },
  where: { token },
});

// 2. Validate that the maxTimestamp plus one day is equal to the min price point
const nextTimestampEpoch = prices[0].timestamp; // assume `prices` has at least one element.
if (nextTimestampEpoch !== unixEpoch(addOneDay(maxTimestamp))) {
  // ----------------------------------------------------------------------
  // Question 1: In what case might the above condition be true? 
  // ----------------------------------------------------------------------
  return;
}

// 3. Perform deletes / creates, given that our data inputs are valid.
const deletedCount = forceRefresh ? (await this.prisma.tokenDailyPriceHistoryUsd.deleteMany({ where: { token } })).count : 0;
const createdCount = (
  await this.prisma.tokenDailyPriceHistoryUsd.createMany({
    data: prices.map(({ price, timestamp }) => ({
      timestamp: new Date(timestamp * 1000),
      price,
      token,
    })),
    skipDuplicates: true,
  })
).count;
