using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;

namespace Hangfire.Mongo.PersistentJobQueue.Mongo
{
    internal class MongoJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly MongoStorage _storage;

        private static readonly EnqueuedAndFetchedCountDto EmptyCounters
            = new EnqueuedAndFetchedCountDto();

        public MongoJobQueueMonitoringApi(MongoStorage storage)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));

            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            return _storage.Connection.JobQueue.AsQueryable()
                .GroupBy(_ => _.Queue)
                .Select(g => g.Key)
                .ToList();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return _storage.Connection.JobQueue.AsQueryable()
                .Where(_ => _.Queue == queue && _.FetchedAt == null)
                .OrderBy(_ => _.Id)
                .Select(_ => _.JobId)
                .Skip(from)
                .Take(perPage)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            // TODO: Hangfire.SqlServer has deprecated dividing queue into enqueued/fetched jobs, probably we should too

            return _storage.Connection.JobQueue.AsQueryable()
                .Where(_ => _.Queue == queue && _.FetchedAt != null)
                .OrderBy(_ => _.Id)
                .Select(_ => _.JobId)
                .Skip(from)
                .Take(perPage)
                .ToList();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            return _storage.Connection.JobQueue.AsQueryable()
                .Where(_ => _.Queue == queue)
                .GroupBy(_ => 1)
                .Select(g => new { Enqueued = g.Count(_ => _.FetchedAt == null), Total = g.Count() })
                .Select(_ => new EnqueuedAndFetchedCountDto { EnqueuedCount = _.Enqueued, FetchedCount = _.Total - _.Enqueued })
                .FirstOrDefault() ?? EmptyCounters;
        }

    }
}