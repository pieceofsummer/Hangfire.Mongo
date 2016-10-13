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
            return _storage.Connection.Job
                .Distinct(Q.DistinctJobQueue, Q.AllJobsWithQueueName)
                .ToList();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return _storage.Connection.Job
                .Find(Q.FindJobsInQueue(queue) & Q.OnlyEnqueuedJobs)
                .Sort(Q.OrderJobById)
                .Skip(from)
                .Limit(perPage)
                .Project(Q.SelectJobId)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            // TODO: Hangfire.SqlServer has deprecated dividing queue into enqueued/fetched jobs, probably we should too

            return _storage.Connection.Job
                .Find(Q.FindJobsInQueue(queue) & Q.OnlyFetchedJobs)
                .Sort(Q.OrderJobById)
                .Skip(from)
                .Limit(perPage)
                .Project(Q.SelectJobId)
                .ToList();
        }
        
        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            var result = _storage.Connection.Job.Aggregate()
                .Match(Q.FindJobsInQueue(queue))
                .Group(Q.CountJobsEnqueuedAndFetched)
                .FirstOrDefault();
                
            if (result == null)
                return EmptyCounters;

            return new EnqueuedAndFetchedCountDto()
            {
                EnqueuedCount = result.Enqueued,
                FetchedCount = result.Total - result.Enqueued
            };
        }
    }
}