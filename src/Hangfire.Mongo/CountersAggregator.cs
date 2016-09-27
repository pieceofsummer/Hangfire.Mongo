using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Server;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Counter collection aggregator for Mongo database
    /// </summary>
#pragma warning disable 618
    public class CountersAggregator : IBackgroundProcess, IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.For<CountersAggregator>();
        
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly MongoStorage _storage;
        private readonly TimeSpan _interval;

        /// <summary>
        /// Constructs Counter collection aggregator
        /// </summary>
        /// <param name="storage">MongoDB storage</param>
        /// <param name="interval">Checking interval</param>
        public CountersAggregator(MongoStorage storage, TimeSpan interval)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            _storage = storage;
            _interval = interval;
        }

        /// <summary>
        /// Runs aggregator
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        /// Runs aggregator
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat("Aggregating records in 'Counter' table...");
            
            long removedCount;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var storageConnection = (MongoConnection)_storage.GetConnection())
                {
                    HangfireDbContext database = storageConnection.Database;

                    var stats = database.Counter.AsQueryable()
                        .Take(NumberOfRecordsInSinglePass)
                        .GroupBy(_ => _.Key)
                        .Select(g => new
                        {
                            Key = g.Key,
                            Ids = g.Select(_ => _.Id),
                            Value = g.Sum(_ => _.Value),
                            ExpireAt = g.Max(_ => _.ExpireAt)
                        })
                        .ToArray();

                    removedCount = 0;

                    foreach (var item in stats)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // update aggregated counter
                        database.AggregatedCounter.UpdateOne(
                            Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, item.Key),
                            Builders<AggregatedCounterDto>.Update.Inc(_ => _.Value, item.Value)
                                                                 .Max(_ => _.ExpireAt, item.ExpireAt),
                            new UpdateOptions { IsUpsert = true }, cancellationToken);

                        // delete corresponding counter records
                        removedCount += database.Counter.DeleteMany(
                            Builders<CounterDto>.Filter.Eq(_ => _.Key, item.Key) &
                            Builders<CounterDto>.Filter.In(_ => _.Id, item.Ids)).DeletedCount;
                    }

                    if (removedCount >= NumberOfRecordsInSinglePass)
                    {
                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                }
                
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_interval);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "MongoDB Counter Colleciton Aggregator";
        }
    }
}