using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Hangfire.Mongo.Dto;

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
            return _storage.Connection.Job
                .Distinct(_ => _.Queue, 
                          Builders<JobDto>.Filter.Exists(_ => _.Queue, true) &
                          Builders<JobDto>.Filter.Ne(_ => _.Queue, null))
                .ToList();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return _storage.Connection.Job
                .Find(_ => _.Queue == queue && _.FetchedAt == null)
                .SortBy(_ => _.Id)
                .Skip(from)
                .Limit(perPage)
                .Project(_ => _.Id)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            // TODO: Hangfire.SqlServer has deprecated dividing queue into enqueued/fetched jobs, probably we should too

            return _storage.Connection.Job
                .Find(_ => _.Queue == queue && _.FetchedAt != null)
                .SortBy(_ => _.Id)
                .Skip(from)
                .Limit(perPage)
                .Project(_ => _.Id)
                .ToList();
        }
        
        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var result = _storage.Connection.Job.Aggregate()
                .Match(_ => _.Queue == queue)
                .Group(_ => 0, g => new
                {
                    EnqueuedCount = g.Sum(_ => (int)(_.FetchedAt ?? (object)1)),
                    TotalCount = g.Count()
                })
                .FirstOrDefault();
                
            if (result == null)
                return EmptyCounters;

            return new EnqueuedAndFetchedCountDto()
            {
                EnqueuedCount = result.EnqueuedCount,
                FetchedCount = result.TotalCount - result.EnqueuedCount
            };
        }
    }
}