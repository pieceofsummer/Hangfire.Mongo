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
            return _storage.Connection.Job.AsQueryable()
                // Required to use index FETCH instead of COLLSCAN
                // (but haven't found a better way to use $exists operator yet)
                .Where(_ => Builders<JobDto>.Filter.Exists("Queue", true).Inject())
                .Where(_ => !string.IsNullOrEmpty(_.Queue))
                .GroupBy(_ => _.Queue)
                .Select(g => g.Key)
                .ToList();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int from, int perPage)
        {
            return _storage.Connection.Job.AsQueryable()
                .Where(_ => _.Queue == queue && _.FetchedAt == null)
                .OrderBy(_ => _.Id)
                .Select(_ => _.Id)
                .Skip(from)
                .Take(perPage)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int from, int perPage)
        {
            // TODO: Hangfire.SqlServer has deprecated dividing queue into enqueued/fetched jobs, probably we should too

            return _storage.Connection.Job.AsQueryable()
                .Where(_ => _.Queue == queue && _.FetchedAt != null)
                .OrderBy(_ => _.Id)
                .Select(_ => _.Id)
                .Skip(from)
                .Take(perPage)
                .ToList();
        }
        
        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            // WARNING: MongoDB Dark Arts follows!
            //
            // Normally we'd use '$cond: [ "$FetchedAt", 1, 0 ]' projection to count enqueued/fetched jobs, 
            // but you cannot write (d => d.FetchedAt ? 1 : 0) in LINQ, as DateTime? cannot be coerced to bool.
            // 
            // You definitely can write (d => d.FetchedAt.HasValue ? 1 : 0) instead, but it translates to a 
            // slightly different query: '$cond: [ { $ne : [ "$FetchedAt", null ] }, 0, 1]', which would also 
            // be fine if not https://jira.mongodb.org/browse/SERVER-13903. Because of that bug, comparison 
            // operators in projections are evaluated incorrectly if the field is missing from document.
            //
            // We still have a few options though:
            // 
            // 1) Use $ifNull in conjunction with $cond, as suggested in the linked bug, which would be: 
            //    (d => (d.FetchedAt ?? null).HasValue ? 0 : 1). 
            //    It will bloat the translated query, but should't affect performance a lot.
            //
            // 2) Calculate a sum over '$ifNull: [ "$FetchedAt", 1 ]' directly. It uses the fact that 
            //    $sum operator ignores non-numeric values, so it will only counts nulls.
            //    Since we're still under the strong typing restrictions, it needs cast to object in projection:
            //    (d => d.FetchedAt ?? (object)1), and then another cast back to int when grouping:
            //    (g => new { Enqueued = g.Sum(_ => (int)_), Fetched = g.Count() - g.Sum(_ => (int)_) }).
            // 
            // 3) THINK OUT OF THE BOX! We're not obliged to always use JobDto to access job collection,
            //    so for the sake of this particular request, let's use a minimalistic copy of JobDto.
            //    It will only include Queue and FetchedAt fields, and FetchedAt will also be BOOLEAN at that!
            //    Since it will be used only for requests, there won't be any troubles with deserialization 
            //    of real DateTime values, while allowing us to write (d => d.FetchedAt ? 0 : 1) as originally planned.
            //
            // Let's proceed with the 2nd way.
            
            return _storage.Connection.Job.AsQueryable()
                .Where(_ => _.Queue == queue)
                .Select(_ => _.FetchedAt ?? (object)1)
                .GroupBy(_ => 0)
                .Select(g => new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = g.Sum(_ => (int)_),
                    FetchedCount = g.Count() - g.Sum(_ => (int)_)
                })
                .FirstOrDefault() ?? EmptyCounters;
        }
    }
}