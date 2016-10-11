using System;
using System.Linq;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.DistributedLock;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Storage;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoDistributedLockFacts
    {
        [Fact]
        public void Acquire_ThrowsAnException_WhenResourceIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>("resource",
                    () => MongoDistributedLock.Acquire(null, TimeSpan.FromSeconds(1), database, new MongoStorageOptions()));
            });
        }

        [Fact]
        public void Acquire_ThrowsAnException_WhenConnectionIsNull()
        {
            Assert.Throws<ArgumentNullException>("database",
                () => MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), null, new MongoStorageOptions()));
        }
        
        [Fact]
        public void Acquire_ThrowsAnException_WhenOptionsIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>("options",
                    () => MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, null));
            });
        }

        [Fact, CleanDatabase]
        public void Acquire_LocksResource_WhenResourceIsFree()
        {
            UseConnection(database =>
            {
                using (MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.AsQueryable()
                        .Where(_ => _.Resource == "resource1")
                        .Count();
                    
                    Assert.Equal(1, locksCount);
                }
            });
        }

        [Fact, CleanDatabase]
        public void Acquire_LocksResource_WhenResourceIsFree_ThenUnlocks_WhenDisposed()
        {
            UseConnection(database =>
            {
                var locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                Assert.Equal(0, locksCount);

                using (MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                    Assert.Equal(1, locksCount);
                }

                locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                Assert.Equal(0, locksCount);
            });
        }

        [Fact, CleanDatabase]
        public void Acquire_IsReentrant_WithinSameExecutionContext()
        {
            UseConnection(database =>
            {
                using (MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                    Assert.Equal(1, locksCount);

                    using (MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                    {
                        locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                        Assert.Equal(1, locksCount);
                    }
                }
            });
        }

        [Fact, CleanDatabase]
        public void Acquire_ThrowsAnException_WhenResourceIsLocked()
        {
            UseConnection(database =>
            {
                var firstAcquired = new ManualResetEvent(false);
                var done = new ManualResetEvent(false);

                using (new ParallelThread(delegate
                {
                    using (MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                    {
                        firstAcquired.Set();
                        done.WaitOne();
                    }
                }))
                using (new ParallelThread(delegate
                {
                    firstAcquired.WaitOne();
                    try
                    {
                        Assert.Throws<DistributedLockTimeoutException>(
                            () => MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()));
                    }
                    finally
                    {
                        done.Set();
                    }
                }))
                {
                    // wait
                }
            });
        }
        
        [Fact, CleanDatabase]
        public void Heartbeat_UpdatesExpireAt_OnLockedResource()
        {
            UseConnection(database =>
            {
                var options = new MongoStorageOptions() { DistributedLockLifetime = TimeSpan.FromSeconds(3) };

                using (MongoDistributedLock.Acquire("resource1", TimeSpan.FromSeconds(1), database, options))
                {
                    DateTime initialExpireAt = database.GetServerTimeUtc() + options.DistributedLockLifetime;
                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    DistributedLockDto lockEntry = database.DistributedLock.AsQueryable().SingleOrDefault(_ => _.Resource == "resource1");
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
}