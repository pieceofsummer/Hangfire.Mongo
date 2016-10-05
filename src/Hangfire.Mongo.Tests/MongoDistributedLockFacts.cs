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
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>("resource",
                    () => new MongoDistributedLock(null, TimeSpan.FromSeconds(1), database, new MongoStorageOptions()));
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            Assert.Throws<ArgumentNullException>("database",
                () => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), null, new MongoStorageOptions()));
        }
        
        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>("options",
                    () => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, null));
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_SetLock_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    var locksCount = database.DistributedLock.AsQueryable()
                        .Where(_ => _.Resource == "resource1")
                        .Count();
                    
                    Assert.Equal(1, locksCount);
                }
            });
        }

        [Fact, CleanDatabase]
        public void Ctor_SetReleaseLock_WhenResourceIsNotLocked()
        {
            UseConnection(database =>
            {
                var locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                Assert.Equal(0, locksCount);

                using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                {
                    locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                    Assert.Equal(1, locksCount);
                }

                locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
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
                    var locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                    Assert.Equal(1, locksCount);

                    using (new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))
                    {
                        locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
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
                    var locksCount = database.DistributedLock.AsQueryable().Count(_ => _.Resource == "resource1");
                    Assert.Equal(1, locksCount);

                    using (new ParallelThread(
                        () => Assert.Throws<DistributedLockTimeoutException>(
                            () => new MongoDistributedLock("resource1", TimeSpan.FromSeconds(1), database, new MongoStorageOptions()))))
                    {
                        // wait
                    }
                }
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