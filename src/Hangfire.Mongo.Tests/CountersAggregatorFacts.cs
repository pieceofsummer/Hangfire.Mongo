using System;
using System.Threading;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using Xunit;
using MongoDB.Driver;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class CountersAggregatorFacts
    {
        [Fact, CleanDatabase]
        public void ThrowsOperationCanceledException_IfTokenCanceled()
        {
            var storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
            using (var connection = (MongoConnection)storage.GetConnection())
            {
                // Arrange
                connection.Database.Counter.InsertOne(new CounterDto
                {
                    Key = "key",
                    Value = 1,
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });

                var aggregator = new CountersAggregator(storage, TimeSpan.Zero);
                var cts = new CancellationTokenSource();
                cts.Cancel();

                // Act
                Assert.Throws<OperationCanceledException>(() => aggregator.Execute(cts.Token));

                // Assert
                Assert.Equal(1, connection.Database.Counter.Count(new BsonDocument()));
                Assert.Equal(0, connection.Database.AggregatedCounter.Count(new BsonDocument()));
            }
        }

        [Fact, CleanDatabase]
        public void AggregatesValueCorrectly()
        {
            var storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
            using (var connection = (MongoConnection)storage.GetConnection())
            {
                // Arrange
                connection.Database.Counter.InsertOne(new CounterDto
                {
                    Key = "key",
                    Value = 1,
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                connection.Database.Counter.InsertOne(new CounterDto
                {
                    Key = "key",
                    Value = 2,
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                connection.Database.Counter.InsertOne(new CounterDto
                {
                    Key = "key",
                    Value = 5,
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });

                var aggregator = new CountersAggregator(storage, TimeSpan.Zero);
                
                // Act
                aggregator.Execute(new CancellationToken());

                // Assert
                Assert.Equal(0, connection.Database.Counter.Count(new BsonDocument()));
                Assert.Equal(1, connection.Database.AggregatedCounter.Count(new BsonDocument()));

                var row = connection.Database.AggregatedCounter.AsQueryable().Single();

                Assert.Equal("key", row.Key);
                Assert.Equal(8, row.Value);
            }
        }

        [Fact, CleanDatabase]
        public void AggregatedExpirationDate_Updates_IfCountersExpireLater()
        {
            var storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
            using (var connection = (MongoConnection)storage.GetConnection())
            {
                var date = DateTime.UtcNow;

                // strip milliseconds, as we lose precision after storing in Mongo
                date = date.AddTicks(-(date.Ticks % TimeSpan.TicksPerSecond));

                // Arrange
                connection.Database.Counter.InsertOne(new CounterDto
                {
                    Key = "key",
                    Value = 1,
                    ExpireAt = date.AddMinutes(5)
                });
                connection.Database.AggregatedCounter.InsertOne(new AggregatedCounterDto
                {
                    Key = "key",
                    Value = 2,
                    ExpireAt = date
                });

                var aggregator = new CountersAggregator(storage, TimeSpan.Zero);

                // Act
                aggregator.Execute(new CancellationToken());

                // Assert
                Assert.Equal(0, connection.Database.Counter.Count(new BsonDocument()));
                Assert.Equal(1, connection.Database.AggregatedCounter.Count(new BsonDocument()));

                var row = connection.Database.AggregatedCounter.AsQueryable().Single();
                
                Assert.Equal(3, row.Value);
                Assert.Equal(date.AddMinutes(5), row.ExpireAt);
            }
        }

        [Fact, CleanDatabase]
        public void AggregatedExpirationDate_DoesNotUpdate_IfCountersExpireBefore()
        {
            var storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
            using (var connection = (MongoConnection)storage.GetConnection())
            {
                var date = DateTime.UtcNow;

                // strip milliseconds, as we lose precision after storing in Mongo
                date = date.AddTicks(-(date.Ticks % TimeSpan.TicksPerSecond));

                // Arrange
                connection.Database.Counter.InsertOne(new CounterDto
                {
                    Key = "key",
                    Value = 1,
                    ExpireAt = date.AddMinutes(-5)
                });
                connection.Database.AggregatedCounter.InsertOne(new AggregatedCounterDto
                {
                    Key = "key",
                    Value = 2,
                    ExpireAt = date
                });

                var aggregator = new CountersAggregator(storage, TimeSpan.Zero);

                // Act
                aggregator.Execute(new CancellationToken());

                // Assert
                Assert.Equal(0, connection.Database.Counter.Count(new BsonDocument()));
                Assert.Equal(1, connection.Database.AggregatedCounter.Count(new BsonDocument()));

                var row = connection.Database.AggregatedCounter.AsQueryable().Single();

                Assert.Equal(3, row.Value);
                Assert.Equal(date, row.ExpireAt);
            }
        }


    }
}