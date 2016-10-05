using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using MongoDB.Bson;
using MongoDB.Driver;
using ServerDto = Hangfire.Storage.Monitoring.ServerDto;

namespace Hangfire.Mongo
{
    internal class MongoMonitoringApi : IMonitoringApi
    {
        private readonly HangfireDbContext _connection;

        private readonly PersistentJobQueueProviderCollection _queueProviders;
        
        public MongoMonitoringApi(HangfireDbContext connection, PersistentJobQueueProviderCollection queueProviders)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (queueProviders == null) throw new ArgumentNullException(nameof(queueProviders));

            _connection = connection;
            _queueProviders = queueProviders;
        }

        public MongoMonitoringApi(MongoStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            _connection = storage.Connection;
            _queueProviders = storage.QueueProviders;
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var tuples = _queueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToArray();

            var result = new List<QueueWithTopEnqueuedJobsDto>(tuples.Length);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var tuple in tuples)
            {
                var enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
                var counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);

                var firstJobs = UseConnection(connection => EnqueuedJobs(connection, enqueuedJobIds));

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = tuple.Queue,
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount,
                    FirstJobs = firstJobs
                });
            }

            return result;
        }

        public IList<ServerDto> Servers()
        {
            var servers = UseConnection(connection => connection.Server.Find(new BsonDocument()).ToList());

            var result = new List<ServerDto>(servers.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var server in servers)
            {
                var data = JobHelper.FromJson<ServerDataDto>(server.Data);
                result.Add(new ServerDto
                {
                    Name = server.Id,
                    Heartbeat = server.LastHeartbeat,
                    Queues = data.Queues,
                    StartedAt = data.StartedAt ?? DateTime.MinValue,
                    WorkersCount = data.WorkerCount
                });
            }

            return result;
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return UseConnection(connection =>
            {
                var job = connection.Job.AsQueryable()
                    .Where(_ => _.Id == jobId)
                    .SingleOrDefault();

                if (job == null)
                    return null;

                var parameters = connection.JobParameter.AsQueryable()
                    .Where(_ => _.JobId == job.Id)
                    .ToDictionary(_ => _.Name, _ => _.Value);

                var history = connection.State.AsQueryable()
                    .Where(_ => _.JobId == job.Id)
                    .OrderByDescending(_ => _.CreatedAt)
                    .ToList()   // materialize
                    .Select(x => new StateHistoryDto
                    {
                        StateName = x.Name,
                        CreatedAt = x.CreatedAt,
                        Reason = x.Reason,
                        Data = JobHelper.FromJson<Dictionary<string, string>>(x.Data)
                    })
                    .ToList();

                return new JobDetailsDto
                {
                    CreatedAt = job.CreatedAt,
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    History = history,
                    Properties = parameters
                };
            });
        }

        public StatisticsDto GetStatistics()
        {
            return UseConnection(connection =>
            {
                var stats = new StatisticsDto();

                var countByState = connection.Job.AsQueryable()
                    .Where(_ => _.StateName != null)
                    .GroupBy(_ => _.StateName)
                    .Select(g => new { g.Key, Value = g.Count() })
                    .ToDictionary(_ => _.Key, _ => _.Value);
                
                stats.Enqueued = countByState.TryGetValue(EnqueuedState.StateName);
                stats.Failed = countByState.TryGetValue(FailedState.StateName);
                stats.Processing = countByState.TryGetValue(ProcessingState.StateName);
                stats.Scheduled = countByState.TryGetValue(ScheduledState.StateName);

                stats.Servers = connection.Server.Count(new BsonDocument());

                using (var cn = new MongoConnection(connection, _queueProviders))
                {
                    stats.Succeeded = cn.GetCounter("stats:succeeded");
                    stats.Deleted = cn.GetCounter("stats:deleted");
                    stats.Recurring = cn.GetSetCount("recurring-jobs");
                }

                stats.Queues = _queueProviders
                    .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                    .Count();

                return stats;
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int count)
        {
            var queueApi = GetQueueApi(queue);
            var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, count);

            return UseConnection(connection => EnqueuedJobs(connection, enqueuedJobIds));
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int count)
        {
            var queueApi = GetQueueApi(queue);
            var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, count);

            return UseConnection(connection => FetchedJobs(connection, fetchedJobIds));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from, count,
                ProcessingState.StateName,
                (detailedJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    ServerId = stateData.TryGetValue("ServerId", (dict, key) => dict["ServerName"]),
                    StartedAt = JobHelper.DeserializeNullableDateTime(stateData.TryGetValue("StartedAt")),
                }));
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, ScheduledState.StateName,
                (detailedJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeNullableDateTime(stateData.TryGetValue("ScheduledAt"))
                }));
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, SucceededState.StateName,
                (detailedJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    Result = stateData.TryGetValue("Result"),
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) + long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData.TryGetValue("SucceededAt"))
                }));
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, FailedState.StateName,
                (detailedJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    Reason = detailedJob.StateReason,
                    ExceptionDetails = stateData.TryGetValue("ExceptionDetails"),
                    ExceptionMessage = stateData.TryGetValue("ExceptionMessage"),
                    ExceptionType = stateData.TryGetValue("ExceptionType"),
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData.TryGetValue("FailedAt"))
                }));
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return UseConnection(connection => GetJobs(connection, from, count, DeletedState.StateName,
                (detailedJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData.TryGetValue("DeletedAt"))
                }));
        }

        public long ScheduledCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ScheduledState.StateName));
        }

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount ?? 0;
        }

        public long FailedCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, FailedState.StateName));
        }

        public long ProcessingCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ProcessingState.StateName));
        }

        public long SucceededListCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, SucceededState.StateName));
        }

        public long DeletedListCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, DeletedState.StateName));
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(connection => GetTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(connection => GetTimelineStats(connection, "failed"));
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(connection => GetHourlyTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(connection => GetHourlyTimelineStats(connection, "failed"));
        }

        private T UseConnection<T>(Func<HangfireDbContext, T> action)
        {
            var result = action(_connection);
            return result;
        }

        private static JobList<EnqueuedJobDto> EnqueuedJobs(HangfireDbContext connection, IEnumerable<string> jobIds)
        {
            var jobs = connection.Job.AsQueryable()
                .Where(_ => jobIds.Contains(_.Id))
                .GroupJoin(connection.State.AsQueryable(), _ => _.StateId, _ => _.Id, (j, s) => new JobDetailedDto
                {
                    Id = j.Id,
                    StateId = j.StateId,
                    StateName = j.StateName,
                    InvocationData = j.InvocationData,
                    Arguments = j.Arguments,
                    CreatedAt = j.CreatedAt,
                    ExpireAt = j.ExpireAt,
                    StateReason = s.First().Reason,
                    StateData = s.First().Data
                })
                .ToDictionary(_ => _.Id);
            
            var orderedJobs = jobIds
                .Select(jobId => jobs.TryGetValue(jobId, _ => new JobDetailedDto { Id = _ }))
                .ToList();

            return DeserializeJobs(orderedJobs,
                (detailedJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = detailedJob.StateName,
                    EnqueuedAt = detailedJob.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(stateData.TryGetValue("EnqueuedAt"))
                        : null
                });
        }

        private static JobList<TDto> DeserializeJobs<TDto>(ICollection<JobDetailedDto> jobs, Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var detailedJob in jobs)
            {
                var job = DeserializeJob(detailedJob.InvocationData, detailedJob.Arguments);
                var stateData = JobHelper.FromJson<Dictionary<string, string>>(detailedJob.StateData);

                var dto = selector(detailedJob, job, stateData);

                result.Add(new KeyValuePair<string, TDto>(detailedJob.Id, dto));
            }

            return new JobList<TDto>(result);
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            var provider = _queueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi();

            return monitoringApi;
        }

        private static JobList<FetchedJobDto> FetchedJobs(HangfireDbContext connection, IEnumerable<string> jobIds)
        {
            var jobs = connection.Job.AsQueryable()
                .Where(_ => jobIds.Contains(_.Id))
                .GroupJoin(connection.State.AsQueryable(), _ => _.StateId, _ => _.Id, (j, s) => new JobDetailedDto
                {
                    Id = j.Id,
                    StateId = j.StateId,
                    StateName = j.StateName,
                    InvocationData = j.InvocationData,
                    Arguments = j.Arguments,
                    CreatedAt = j.CreatedAt,
                    ExpireAt = j.ExpireAt,
                    StateReason = s.First().Reason,
                    StateData = s.First().Data
                })
                .ToList();
            
            var result = new List<KeyValuePair<string, FetchedJobDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                result.Add(new KeyValuePair<string, FetchedJobDto>(
                    job.Id,
                    new FetchedJobDto
                    {
                        Job = DeserializeJob(job.InvocationData, job.Arguments),
                        State = job.StateName
                    }));
            }

            return new JobList<FetchedJobDto>(result);
        }

        private static JobList<TDto> GetJobs<TDto>(HangfireDbContext connection, int from, int count, string stateName, Func<JobDetailedDto, Job, Dictionary<string, string>, TDto> selector)
        {
            var jobs = connection.Job.AsQueryable()
                .Where(_ => _.StateName == stateName)
                .OrderByDescending(_ => _.Id)
                .Skip(from)
                .Take(count)
                .GroupJoin(connection.State.AsQueryable(), _ => _.StateId, _ => _.Id, (j, s) => new JobDetailedDto
                {
                    Id = j.Id,
                    StateId = j.StateId,
                    StateName = j.StateName,
                    InvocationData = j.InvocationData,
                    Arguments = j.Arguments,
                    CreatedAt = j.CreatedAt,
                    ExpireAt = j.ExpireAt,
                    StateReason = s.First().Reason,
                    StateData = s.First().Data
                })
                .ToList();
            
            return DeserializeJobs(jobs, selector);
        }
        
        private static long GetNumberOfJobsByStateName(HangfireDbContext connection, string stateName)
        {
            var count = connection.Job.Count(Builders<JobDto>.Filter.Eq(_ => _.StateName, stateName));
            return count;
        }

        private Dictionary<DateTime, long> GetTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.Database.GetServerTimeUtc().Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => $"stats:{type}:{x}").ToList();

            var valuesMap = connection.AggregatedCounter
                .Find(Builders<AggregatedCounterDto>.Filter.In(_ => _.Key, keys))
                .ToList()
                .GroupBy(x => x.Key)
                .ToDictionary(x => x.Key, x => (long)x.Count());

            foreach (var key in keys)
            {
                if (!valuesMap.ContainsKey(key)) valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < stringDates.Count; i++)
            {
                var value = valuesMap[valuesMap.Keys.ElementAt(i)];
                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.Database.GetServerTimeUtc();
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => $"stats:{type}:{x:yyyy-MM-dd-HH}").ToList();

            var valuesMap = connection.Counter.Find(Builders<CounterDto>.Filter.In(_ => _.Key, keys))
                .ToList()
                .GroupBy(x => x.Key, x => x)
                .ToDictionary(x => x.Key, x => (long)x.Count());

            foreach (var key in keys.Where(key => !valuesMap.ContainsKey(key)))
            {
                valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                var value = valuesMap[valuesMap.Keys.ElementAt(i)];
                result.Add(dates[i], value);
            }

            return result;
        }
    }
}