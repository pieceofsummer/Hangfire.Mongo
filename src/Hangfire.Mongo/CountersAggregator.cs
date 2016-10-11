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

            using (var storageConnection = (MongoConnection)_storage.GetConnection())
            {
                HangfireDbContext database = storageConnection.Database;

                var stats = database.Counter.Aggregate()
                    .Group(_ => _.Key, g => new
                    {
                        Key = g.Key,
                        // Hangfire's collections shouldn't be sharded!
                        // so max(id) is a good enough boundary
                        LastId = g.Max(_ => _.Id),
                        Value = g.Sum(_ => _.Value),
                        ExpireAt = g.Max(_ => _.ExpireAt)
                    })
                    .ToList();

                foreach (var item in stats)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // update aggregated counter
                    database.AggregatedCounter.UpdateOne(
                        Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, item.Key),
                        Builders<AggregatedCounterDto>.Update.Inc(_ => _.Value, item.Value)
                                                             .Max(_ => _.ExpireAt, item.ExpireAt),
                        new UpdateOptions { IsUpsert = true });

                    // delete corresponding counter records
                    database.Counter.DeleteMany(
                        Builders<CounterDto>.Filter.Eq(_ => _.Key, item.Key) &
                        Builders<CounterDto>.Filter.Lte(_ => _.Id, item.LastId));
                }
            }

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