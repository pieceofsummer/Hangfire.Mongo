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
            var queues = UseConnection(connection => connection.Job
                .Distinct(Q.DistinctJobQueue, Q.AllJobsWithQueueName)
                .ToList());

            var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var queue in queues)
            {
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
                    .Find(Q.FindJobById(jobId))
                    .Project(Q.SelectJobAsPartial)
                    .SingleOrDefault();

                if (jobData == null)
                    return null;

                var jobParameters = connection.JobParameter
                    .Find(Q.FindJobParametersByJobId(jobData.Id))
                    .Project(Q.SelectJobParameterNameAndValue)
                    .ToDictionary(_ => _.Name, _ => _.Value);

                var jobHistory = connection.State
                    .Find(Q.FindStatesByJobId(jobData.Id))
                    .Sort(Q.OrderStateByCreatedAtDescending)
                    .Project(Q.SelectStateForHistory)
                    .ToList();

                return new JobDetailsDto
                {
                    Job = DeserializeJob(jobData.InvocationData, jobData.Arguments),
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
                var jobsByStateName = connection.Job.Aggregate()
                    .Match(Q.AllJobsInScheduledEnqueuedFailedProcessingStates)
                    .Group(Q.GroupJobsByStateName)
                    .ToDictionary(_ => _.Key, _ => _.Value);

                var countersByKey = connection.Counter.Aggregate()
                    .Match(Q.SucceededAndDeletedCounters)
                    .Group(Q.GroupCountersByKey)
                    .ToDictionary(_ => _.Key, _ => _.Value);

                var aggregatedByKey = connection.AggregatedCounter
                    .Find(Q.SucceededAndDeletedAggregatedCounters)
                    .ToDictionary(_ => _.Key, _ => _.Value);

                // ReSharper disable once UseObjectOrCollectionInitializer
                var stats = new StatisticsDto();

                stats.Enqueued = jobsByStateName.TryGetValue(EnqueuedState.StateName);
                stats.Failed = jobsByStateName.TryGetValue(FailedState.StateName);
                stats.Processing = jobsByStateName.TryGetValue(ProcessingState.StateName);
                stats.Scheduled = jobsByStateName.TryGetValue(ScheduledState.StateName);
                stats.Servers = connection.Server.Count();
                stats.Succeeded = aggregatedByKey.TryGetValue(Q.SucceededCounterKey)
                                  + countersByKey.TryGetValue(Q.SucceededCounterKey);
                stats.Deleted = aggregatedByKey.TryGetValue(Q.DeletedCounterKey)
                                + countersByKey.TryGetValue(Q.DeletedCounterKey);
                stats.Recurring = connection.Set.Count(Q.FindRecurringJobsSet);

                stats.Queues = _queueProviders
                    .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                    .Count();

                return stats;
            });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int count)
        {
            return UseConnection(connection => GetJobsByQueue(
                connection,
                from, count,
                queue, false,
                job => new EnqueuedJobDto
                {
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    State = job.StateName,
                    EnqueuedAt = job.StateName == EnqueuedState.StateName
                        ? JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("EnqueuedAt"))
                        : null
                }));
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int count)
        {
            return UseConnection(connection => GetJobsByQueue(
                connection,
                from, count,
                queue, true,
                job => new FetchedJobDto
                {
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
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
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
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
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
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
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
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
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
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
                    Job = DeserializeJob(job.InvocationData, job.Arguments),
                    DeletedAt = JobHelper.DeserializeNullableDateTime(job.StateData.TryGetValue("DeletedAt"))
                }));
        }

        public long ScheduledCount()
        {
            return UseConnection(connection => GetNumberOfJobsByStateName(connection, ScheduledState.StateName));
        }

        public long EnqueuedCount(string queue)
        {
            return UseConnection(connection => GetNumberOfJobsByQueue(connection, queue, false));
        }

        public long FetchedCount(string queue)
        {
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
        
        private static Job DeserializeJob(string invocationData, string arguments)
        {
            if (string.IsNullOrEmpty(invocationData))
                return null;

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

        private static FilterDefinition<JobDto> GetQueuePredicate(string queue, bool fetched)
        {
            return Q.FindJobsInQueue(queue) & (fetched ? Q.OnlyFetchedJobs : Q.OnlyEnqueuedJobs);
        }

        private static JobList<TDto> GetJobsByQueue<TDto>(HangfireDbContext connection, int from, int count, string queue, bool fetched, Func<JobDto, TDto> selector)
        {
            var jobs = connection.Job
                .Find(GetQueuePredicate(queue, fetched))
                .Sort(Q.OrderJobById)
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
                .Find(Q.FindJobsInState(stateName))
                .Sort(Q.OrderJobByIdDescending)
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
            var count = connection.Job.Count(Q.FindJobsInState(stateName));
            return count;
        }

        private static Dictionary<DateTime, long> GetTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.GetServerTimeUtc().Date;

            var dates = new List<DateTime>(7);
            var keys = new List<string>(7);

            for (int i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                keys.Add($"stats:{type}:{endDate:yyyy-MM-dd}");
                endDate = endDate.AddDays(-1);
            }
            
            var values = connection.AggregatedCounter
                .Find(Q.FindAggregatedCountersByKeys(keys))
                .ToDictionary(x => x.Key, x => x.Value);

            var extras = connection.Counter.Aggregate()
                .Match(Q.FindCountersByKey(keys[0]))
                .Group(Q.GroupCountersByKey)
                .ToDictionary(_ => _.Key, _ => _.Value);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                result.Add(dates[i], values.TryGetValue(keys[i]) + extras.TryGetValue(keys[i]));
            }

            return result;
        }

        private static Dictionary<DateTime, long> GetHourlyTimelineStats(HangfireDbContext connection, string type)
        {
            var endDate = connection.GetServerTimeUtc();

            var dates = new List<DateTime>(24);
            var keys = new List<string>(24);

            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                keys.Add($"stats:{type}:{endDate:yyyy-MM-dd-HH}");
                endDate = endDate.AddHours(-1);
            }
            
            var values = connection.AggregatedCounter
                .Find(Q.FindAggregatedCountersByKeys(keys))
                .ToDictionary(x => x.Key, x => x.Value);
            
            var extras = connection.Counter.Aggregate()
                .Match(Q.FindCountersByKey(keys[0]))
                .Group(Q.GroupCountersByKey)
                .ToDictionary(_ => _.Key, _ => _.Value);
            
            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                result.Add(dates[i], values.TryGetValue(keys[i]) + extras.TryGetValue(keys[i]));
            }

            return result;
        }
    }
}