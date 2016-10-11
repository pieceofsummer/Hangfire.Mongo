using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Storage;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Hangfire.Mongo.DistributedLock
{

    /// <summary>
    /// Distributed lock implementation
    /// </summary>
    public class MongoDistributedLock : IDisposable
    {
        private static readonly ILog _logger = LogProvider.For<MongoDistributedLock>();

        #region LockId generation

        /// <summary>
        /// Unique prefix for the current host/process
        /// </summary>
        private static readonly string _processIdPrefix;

        /// <summary>
        /// Process-local lock counter
        /// </summary>
        private static long _lockIdCounter = 0;
        
        static MongoDistributedLock()
        {
            using (var s = new MemoryStream())
            using (var w = new StreamWriter(s, Encoding.UTF8))
            {
                // append machine name
                w.Write(Environment.MachineName);

                // append process id
                try
                {
                    w.Write(GetCurrentProcessId());
                }
                catch (SecurityException)
                {
                    // failed to get process id :(
                    w.Write(0);
                }

                // add even more uniqueness
                w.Write(Environment.TickCount);
                
                s.Seek(0, SeekOrigin.Begin);
                using (var sha1 = SHA1.Create())
                {
                    _processIdPrefix = MongoDB.Bson.BsonUtils.ToHexString(sha1.ComputeHash(s)) + ":";
                }
            }
        }

        /// <summary>
        /// Returns the identifier of the current process
        /// </summary>
        /// <remarks>
        /// Do not inline it! Otherwise we won't be able to catch 
        /// <see cref="SecurityException"/> inside the caller method
        /// </remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static int GetCurrentProcessId()
        {
            return Process.GetCurrentProcess().Id;
        }

        /// <summary>
        /// Generates a unique identifier for lock owner
        /// </summary>
        /// <returns></returns>
        private static string NewLockId()
        {
            var counter = Interlocked.Increment(ref _lockIdCounter);
            return $"{_processIdPrefix}{counter}";
        }

        #endregion

        #region AcquiredLocks tracking

        private static readonly AsyncLocal<ISet<string>> _acquiredLocks = new AsyncLocal<ISet<string>>();
        
        /// <summary>
        /// Returns a set of resources locked by the current execution context
        /// </summary>
        protected static ISet<string> AcquiredLocks
        {
            get
            {
                var value = _acquiredLocks.Value;
                if (value == null)
                {
                    value = new HashSet<string>();
                    _acquiredLocks.Value = value;
                }
                return value;
            }
        }

        #endregion

        private readonly string _resource;

        private readonly string _lockId;

        private readonly HangfireDbContext _database;

        private readonly MongoStorageOptions _options;

        private readonly InProcessLockAwaiter _inProcAwaiter;

        private readonly Timer _heartbeatTimer;

        private bool _disposed = false;

        /// <summary>
        /// Initializes an instance of distributed lock object
        /// </summary>
        /// <param name="resource">Resource name</param>
        /// <param name="lockId">Unique lock id</param>
        /// <param name="database">Mongo database context</param>
        /// <param name="options">Mongo storage options</param>
        private MongoDistributedLock(string resource, string lockId, HangfireDbContext database, MongoStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource))
                throw new ArgumentNullException(nameof(resource));
            if (string.IsNullOrEmpty(lockId))
                throw new ArgumentNullException(nameof(lockId));
            if (database == null)
                throw new ArgumentNullException(nameof(database));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _resource = resource;
            _lockId = lockId;
            _database = database;
            _options = options;
            
            _inProcAwaiter = InProcessLockAwaiter.GetAwaiter(_resource);
            _inProcAwaiter.Reset();

            AcquiredLocks.Add(_resource);

            var interval = TimeSpan.FromMilliseconds(_options.DistributedLockLifetime.TotalMilliseconds / 5);
            _heartbeatTimer = new Timer(HeartbeatTimerCallback, null, interval, interval);
        }

        /// <summary>
        /// Releases the resource when the lock instance is disposed
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;

            _disposed = true;

            _heartbeatTimer.Dispose();

            try
            {
                var r = _database.DistributedLock.DeleteOne(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) &
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Owner, _lockId));

                if (r.DeletedCount == 0)
                {
                    // Resource was stolen from us?
                    throw new MongoDistributedLockException($"Ownership for '{_resource}' was lost");
                }

                _logger.Debug($"Lock '{_lockId}' released '{_resource}'");
            }
            catch (MongoDistributedLockException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.ErrorException($"Lock '{_lockId}' failed to release '{_resource}'", ex);
                throw new MongoDistributedLockException($"Failed to release '{_resource}'", ex);
            }
            finally
            {
                AcquiredLocks.Remove(_resource);
                _inProcAwaiter.Signal();
            }
        }
        
        private void HeartbeatTimerCallback(object state)
        {
            if (_disposed) return;

            Heartbeat();
        }

        /// <summary>
        /// Update heartbeat for this lock
        /// </summary>
        private void Heartbeat()
        {
            if (_disposed)
                throw new ObjectDisposedException(_lockId);

            _logger.Debug($"Lock '{_lockId}' heartbeat");

            try
            {
                var r = _database.DistributedLock.UpdateOne(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, _resource) &
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Owner, _lockId),
                    Builders<DistributedLockDto>.Update.Set(_ => _.ExpireAt, _database.GetServerTimeUtc() + _options.DistributedLockLifetime));

                if (r.MatchedCount == 0)
                {
                    // Resource was stolen from us?
                    _heartbeatTimer.Dispose();
                }
            }
            catch (Exception ex)
            {
                _logger.ErrorException($"Failed to update lock expiration for '{_resource}'", ex);
            }
        }
        
        private static LockStatus TryAcquireLock(string resource, string ownerId, HangfireDbContext database, MongoStorageOptions options)
        {
            try
            {
                database.DistributedLock.DeleteOne(
                    Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource) &
                    Builders<DistributedLockDto>.Filter.Lt(_ => _.ExpireAt, database.GetServerTimeUtc()));
            }
            catch (Exception ex)
            {
                _logger.WarnException($"Failed to remove expired lock for '{resource}'", ex);
            }
            
            var before = database.DistributedLock.FindOneAndUpdate(
                Builders<DistributedLockDto>.Filter.Eq(_ => _.Resource, resource),
                Builders<DistributedLockDto>.Update.SetOnInsert(_ => _.Owner, ownerId)
                                                   .SetOnInsert(_ => _.ExpireAt, database.GetServerTimeUtc() + options.DistributedLockLifetime),
                new FindOneAndUpdateOptions<DistributedLockDto, DistributedLockDto>()
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.Before
                });

            if (before == null)
            {
                return LockStatus.Acquired;
            }
            else
            {
                return new LockStatus(before.Owner.StartsWith(_processIdPrefix), before.ExpireAt - database.GetServerTimeUtc());
            }
        }
        
        /// <summary>
        /// Acquires the distributed lock for the specified resource
        /// </summary>
        /// <param name="resource">Resource name</param>
        /// <param name="timeout">Lock acquisition timeout</param>
        /// <param name="database">Mongo database context</param>
        /// <param name="options">Mongo storage options</param>
        /// <returns>Distriuted lock for the specified resource</returns>
        /// <exception cref="ArgumentNullException">One of required arguments is null</exception>
        /// <exception cref="DistributedLockTimeoutException">Failed to acquire the lock in time</exception>
        /// <exception cref="MongoDistributedLockException">Other implementation-specific errors</exception>
        public static IDisposable Acquire(string resource, TimeSpan timeout, HangfireDbContext database, MongoStorageOptions options)
        {
            if (string.IsNullOrEmpty(resource))
                throw new ArgumentNullException(nameof(resource));
            if (database == null)
                throw new ArgumentNullException(nameof(database));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            if (AcquiredLocks.Contains(resource))
            {
                // substitute with a no-op implementation
                return new NoOpDistributedLock();
            }

            var lockId = NewLockId();

            var remaining = timeout;

            _logger.Debug($"Lock '{lockId}' trying to acquire '{resource}'");

            var time = DateTime.UtcNow;
            var result = TryAcquireLock(resource, lockId, database, options);
            while (result.IsOccupied)
            {
                ILockAwaiter awaiter;
                if (result.IsInProcess)
                {
                    awaiter = InProcessLockAwaiter.GetAwaiter(resource);
                }
                /* else if (database.IsOplogEnabled)
                {
                    awaiter = new OplogLockAwaiter(resource, database);
                }*/
                else
                {
                    awaiter = new PollingLockAwaiter(timeout, result.Timeout);
                }

                // calculate remaining time
                remaining -= (DateTime.UtcNow - time);

                _logger.Debug($"Lock '{lockId}' waiting for occupied '{resource}'...");

                if (!awaiter.Wait(remaining))
                {
                    // no luck
                    _logger.Debug($"Lock '{lockId}' failed to acquire '{resource}' in time");
                    throw new DistributedLockTimeoutException(resource);
                }

                _logger.Debug($"Lock '{lockId}' retrying to acquire '{resource}'");
                
                time = DateTime.UtcNow;
                result = TryAcquireLock(resource, lockId, database, options);
            }

            _logger.Debug($"Lock '{lockId}' acquired '{resource}'");

            return new MongoDistributedLock(resource, lockId, database, options);
        }
    }
}
