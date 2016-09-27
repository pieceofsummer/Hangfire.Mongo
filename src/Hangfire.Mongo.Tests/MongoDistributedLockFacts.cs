﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Storage;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoDistributedLockFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock(null, TimeSpan.FromSeconds(1), database, new MongoStorageOptions()));

                Assert.Equal("resource", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), null, new MongoStorageOptions()));

            Assert.Equal("database", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_SetLock_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.Count(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                    Assert.Equal(1, locksCount);
                }
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                long locksCount;
                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    locksCount = database.DistributedLock.Count(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                    Assert.Equal(1, locksCount);
                }

                locksCount = database.DistributedLock.Count(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                Assert.Equal(0, locksCount);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_AcquireLockWithinSameThread_WhenResourceIsLocked()
        {
            UseConnection(database =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.Count(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                    Assert.Equal(1, locksCount);

                    using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                    {
                        locksCount = database.DistributedLock.Count(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                        Assert.Equal(1, locksCount);
                    }
                }
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenResourceIsLocked()
        {
            UseConnection(database =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.Count(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1"));
                    Assert.Equal(1, locksCount);

                    Task.Run(() =>
                    {
                        Assert.Throws<DistributedLockTimeoutException>(() => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()));
                    }).Wait();
                }
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            UseConnection(database =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, null));

                Assert.Equal("options", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_SetLockHeartbeatWorks_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                var options = new MongoStorageOptions() { DistributedLockLifetime = TimeSpan.FromSeconds(3) };

                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, options))
                {
                    DateTime initialExpireAt = database.GetServerTimeUtc() + options.DistributedLockLifetime;
                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    DistributedLockDto lockEntry = database.DistributedLock.Find(Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, "resource1")).FirstOrDefault();
                    Assert.NotNull(lockEntry);
                    Assert.True(lockEntry.ExpireAt > initialExpireAt);
                }
            });
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
#pragma warning restore 1591
}