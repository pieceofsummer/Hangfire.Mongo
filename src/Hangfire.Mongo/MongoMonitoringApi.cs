using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using MongoDB.Driver;
using ServerDto = Hangfire.Storage.Monitoring.ServerDto;
using System.Linq.Expressions;
using MongoDB.Driver.Linq;
using System.Collections;

namespace Hangfire.Mongo
{
    internal class MongoMonitoringApi : IMonitoringApi
    {
        private readonly HangfireDbContext _connection;

        private readonly PersistentJobQueueProviderCollection _queueProviders;

        private static readonly string[] StatisticsStateNames = new[]
        {
            ScheduledState.StateName,
            EnqueuedState.StateName,
            FailedState.StateName,
            ProcessingState.StateName,
        };

        private static readonly string[] StatisticsCounters = new[]
        {
            "stats:succeeded",
            "stats:deleted"
        };

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
            /*var tuples = _queueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToList();*/

            var queues = UseConnection(connection => connection.Job
                .Distinct(_ => _.Queue,
                          Builders<JobDto>.Filter.Exists(_ => _.Queue, true) &
                          Builders<JobDto>.Filter.Ne(_ => _.Queue, null))
                .ToList());

            var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var queue in queues)
            {
                /*var enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
                var counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);

                var firstJobs = UseConnection(connection => EnqueuedJobs(connection, enqueuedJobIds));*/

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = queue,
                    Length = EnqueuedCount(queue),
                    Fetched = FetchedCount(queue),
                    FirstJobs = EnqueuedJobs(queue, 0, 5)
                });
            }

            return result;
        }

        public IList<ServerDto> Servers()
        {
            var servers = UseConnection(connection => connection.Server.AllDocuments().ToList());

            var result = new List<ServerDto>(servers.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var server in servers)
            {
                result.Add(new ServerDto
                {
                    Name = server.Name,
                    Heartbeat = server.Heartbeat,
                    Queues = server.Queues,
                    StartedAt = server.StartedAt,
                    WorkersCount = server.WorkerCount
                });
            }

            return result;
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return UseConnection(connection =>
            {
                var jobData = connection.Job
                    .Find(_ => _.Id == jobId)
                    .Project(_ => new { _.Id, _.CreatedAt, _.InvocationData, _.Arguments })
                    .SingleOrDefault();

                if (jobData == null)
                    return null;

                var jobParameters = connection.JobParameter
                    .Find(_ => _.JobId == jobData.Id)
                    .Project(_ => new { _.Name, _.Value })
                    .ToDictionary(_ => _.Name, _ => _.Value);

                var jobHistory = connection.State
                    .Find(_ => _.JobId == jobData.Id)
                    .SortByDescending(_ => _.CreatedAt)
                    .Project(_ => new StateHistoryDto
                    {
                        StateName = _.Name,
                        CreatedAt = _.CreatedAt,
                        Reason = _.Reason,
                        Data = _.Data
                    })
                    .ToList();

                return new JobDetailsDto
                {
                    Job = JobDto.Deserialize(jobData.InvocationData, jobData.Arguments),
                    CreatedAt = jobData.CreatedAt,
                    History = jobHistory,
                    Properties = jobParameters
                };
            });
        }

        public StatisticsDto GetStatistics()
        {
            return UseConnection(connection =>
            {
                var statesByName = connection.Job.Aggregate()
                    .Match(_ => StatisticsStateNames.Contains(_.StateName))
                    .Group(_ => _.StateName, g => new { g.Key, Value = g.Count() })
                    .ToDictionary(_ => _.Key, _ => _.Value);

                var countersByKey = connection.Counter.Aggregate()
                    .Match(_ => StatisticsCounters.Contains(_.Key))
                    .Group(_ => _.Key, g => new { g.Key, Value = g.Sum(_ => _.Value) })
                    .ToDictionary(_ => _.Key, _ => _.Value);

                var aggregatedByKey = connection.AggregatedCounter
                    .Find(_ => StatisticsCounters.Contains(_.Key))
                    .ToDictionary(_ => _.Key, _ => _.Value);

                var stats = new StatisticsDto();

                stats.Enqueued = statesByName.TryGetValue(EnqueuedState.StateName);
                stats.Failed = statesByName.TryGetValue(FailedState.StateName);
                stats.Processing = statesByName.TryGetValue(ProcessingState.StateName);
                stats.Scheduled = statesByName.TryGetValue(ScheduledState.StateName);
                stats.Servers = connection.Server.Count();
                stats.Succeeded = aggregatedByKey.TryGetValue(StatisticsCounters[0]) + 
                                    countersByKey.TryGetValue(StatisticsCounters[0]);
                stats.Deleted = aggregatedByKey.TryGetValue(StatisticsCounters[1]) + 
                                  countersByKey.TryGetValue(StatisticsCounters[1]);
                stats.Recurring = connection.Set.Count(_ => _.Key == "recurring-jobs");

                stats.Queues = _queueProviders
                    .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                    .Count();
                
                return stats;
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int count)
        {
            /*var queueApi = GetQueueApi(queue);
            var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, count);

            return UseConnection(connection => EnqueuedJobs(connection, enqueuedJobIds));*/

            return UseConnection(connection => GetJobsByQueue(
                connection,
                from, count,
                queue, false,
                job => new EnqueuedJobDto
                {
                    Job = job.Deserialize(),
                    State = job.StateName,
                    EnqueuedAt = job.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("EnqueuedAt"))
                        : null
                }));
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int count)
        {
            /*var queueApi = GetQueueApi(queue);
            var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, count);

            return UseConnection(connection => FetchedJobs(connection, fetchedJobIds));*/

            return UseConnection(connection => GetJobsByQueue(
                connection,
                from, count,
                queue, true,
                job => new FetchedJobDto
                {
                    Job = job.Deserialize(),
                    State = job.StateName,
                    FetchedAt = job.FetchedAt
                }));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return UseConnection(connection => GetJobsByStateName(
                connection,
                from, count,
                ProcessingState.StateName,
                job => new ProcessingJobDto
                {
                    Job = job.Deserialize(),
                    ServerId = job.StateData.TryGetValue("ServerId") ?? job.StateData.TryGetValue("ServerName"),
                    StartedAt = JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("StartedAt")),
                }));
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return UseConnection(connection => GetJobsByStateName(
                connection, 
                from, count, 
                ScheduledState.StateName,
                job => new ScheduledJobDto
                {
                    Job = job.Deserialize(),
                    EnqueueAt = JobHelper.DeserializeDateTime(job.StateData["EnqueueAt"]),
                    ScheduledAt = JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("ScheduledAt"))
                }));
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(connection => GetJobsByStateName(
                connection, 
                from, count, 
                SucceededState.StateName,
                job => new SucceededJobDto
                {
                    Job = job.Deserialize(),
                    Result = job.StateData.TryGetValue("Result"),
                    TotalDuration = job.StateData != null && 
                        job.StateData.ContainsKey("PerformanceDuration") && 
                        job.StateData.ContainsKey("Latency")
                        ? long.Parse(job.StateData["PerformanceDuration"]) + long.Parse(job.StateData["Latency"])
                        : (long?)null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("SucceededAt"))
                }));
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(connection => GetJobsByStateName(
                connection, 
                from, count, 
                FailedState.StateName,
                job => new FailedJobDto
                {
                    Job = job.Deserialize(),
                    Reason = job.StateReason,
                    ExceptionDetails = job.StateData.TryGetValue("ExceptionDetails"),
                    ExceptionMessage = job.StateData.TryGetValue("ExceptionMessage"),
                    ExceptionType = job.StateData.TryGetValue("ExceptionType"),
                    FailedAt = JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("FailedAt"))
                }));
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return UseConnection(connection => GetJobsByStateName(
                connection, 
                from, count, 
                DeletedState.StateName,
                job => new DeletedJobDto
                {
                    Job = job.Deserialize(),
                    DeletedAt = JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("DeletedAt"))
                }));
        }

        public long ScheduledCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ScheduledState.StateName));
        }

        public long EnqueuedCount(string queue)
        {
            /*var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount;*/

            return UseConnection(connection => GetNumberOfJobsByQueue(connection, queue, false));
        }

        public long FetchedCount(string queue)
        {
            /*var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount;*/

            return UseConnection(connection => GetNumberOfJobsByQueue(connection, queue, true));
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
            var jobs = connection.Job
                .Find(_ => jobIds.Contains(_.Id))
                .ToList();

            var result = new List<KeyValuePair<string, EnqueuedJobDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                result.Add(new KeyValuePair<string, EnqueuedJobDto>(
                    job.Id,
                    new EnqueuedJobDto
                    {
                        Job = job.Deserialize(),
                        State = job.StateName,
                        EnqueuedAt = job.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("EnqueuedAt"))
                        : null
                    }));
            }

            return new JobList<EnqueuedJobDto>(result);
        }
        
        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            var provider = _queueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi();

            return monitoringApi;
        }

        private static JobList<FetchedJobDto> FetchedJobs(HangfireDbContext connection, IEnumerable<string> jobIds)
        {
            var jobs = connection.Job
                .Find(_ => jobIds.Contains(_.Id))
                .ToList();
            
            var result = new List<KeyValuePair<string, FetchedJobDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                result.Add(new KeyValuePair<string, FetchedJobDto>(
                    job.Id,
                    new FetchedJobDto
                    {
                        Job = job.Deserialize(),
                        State = job.StateName,
                        FetchedAt = job.FetchedAt
                    }));
            }

            return new JobList<FetchedJobDto>(result);
        }

        private static FilterDefinition<JobDto> GetQueuePredicate(string queue, bool fetched)
        {
            var builder = Builders<JobDto>.Filter;

            return builder.Eq(_ => _.Queue, queue) & (fetched 
                ? builder.Ne(_ => _.FetchedAt, null) 
                : builder.Eq(_ => _.FetchedAt, null));
        }

        private static JobList<TDto> GetJobsByQueue<TDto>(HangfireDbContext connection, int from, int count, string queue, bool fetched, Func<JobDto, TDto> selector)
        {
            var jobs = connection.Job
                .Find(GetQueuePredicate(queue, fetched))
                .SortBy(_ => _.Id)
                .Skip(from)
                .Limit(count)
                .ToList();

            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                var dto = selector(job);

                result.Add(new KeyValuePair<string, TDto>(job.Id, dto));
            }

            return new JobList<TDto>(result);
        }

        private static long GetNumberOfJobsByQueue(HangfireDbContext connection, string queue, bool fetched)
        {
            var count = connection.Job.Count(GetQueuePredicate(queue, fetched));
            return count;
        }

        private static JobList<TDto> GetJobsByStateName<TDto>(HangfireDbContext connection, int from, int count, string stateName, Func<JobDto, TDto> selector)
        {
            var jobs = connection.Job
                .Find(_ => _.StateName == stateName)
                .SortByDescending(_ => _.Id)
                .Skip(from)
                .Limit(count)
                .ToList();

            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                var dto = selector(job);

                result.Add(new KeyValuePair<string, TDto>(job.Id, dto));
            }

            return new JobList<TDto>(result);
        }
        
        private static long GetNumberOfJobsByStateName(HangfireDbContext connection, string stateName)
        {
            var count = connection.Job.Count(_ => _.StateName == stateName);
            return count;
        }

        private Dictionary<DateTime, long> GetTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.GetServerTimeUtc().Date;

            var dates = new List<DateTime>(7);
            var keys = new List<string>(7);

            for (int i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                keys.Add($"stats:{type}:{endDate.ToString("yyyy-MM-dd")}");
                endDate = endDate.AddDays(-1);
            }
            
            var values = connection.AggregatedCounter
                .Find(_ => keys.Contains(_.Key))
                .ToDictionary(x => x.Key, x => x.Value);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                result.Add(dates[i], values.TryGetValue(keys[i]));
            }

            return result;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.GetServerTimeUtc();

            var dates = new List<DateTime>(24);
            var keys = new List<string>(24);

            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                keys.Add($"stats:{type}:{endDate.ToString("yyyy-MM-dd-HH")}");
                endDate = endDate.AddHours(-1);
            }
            
            var values = connection.AggregatedCounter
                .Find(_ => keys.Contains(_.Key))
                .ToDictionary(x => x.Key, x => x.Value);
            
            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                result.Add(dates[i], values.TryGetValue(keys[i]));
            }

            return result;
        }
    }
}