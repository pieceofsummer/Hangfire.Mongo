using System;
using System.Diagnostics;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.States;
using Hangfire.Storage;
using Xunit;
using MongoDB.Bson;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoMonitoringApiFacts
    {
        private const string DefaultQueue = "default";
        private const string FetchedStateName = "Fetched";
        private const int From = 0;
        private const int PerPage = 5;
        
        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsZero_WhenNoJobsExist()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var result = monitoringApi.GetStatistics();
                Assert.Equal(0, result.Enqueued);
                Assert.Equal(0, result.Failed);
                Assert.Equal(0, result.Processing);
                Assert.Equal(0, result.Scheduled);
            });
        }

        [Fact, CleanDatabase]
        public void GetStatistics_ReturnsExpectedCounts_WhenJobsExist()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                CreateJobInState(database, null, EnqueuedState.StateName);
                CreateJobInState(database, null, EnqueuedState.StateName);
                CreateJobInState(database, null, FailedState.StateName);
                CreateJobInState(database, null, ProcessingState.StateName);
                CreateJobInState(database, null, ScheduledState.StateName);
                CreateJobInState(database, null, ScheduledState.StateName);

                var result = monitoringApi.GetStatistics();
                Assert.Equal(2, result.Enqueued);
                Assert.Equal(1, result.Failed);
                Assert.Equal(1, result.Processing);
                Assert.Equal(2, result.Scheduled);
            });
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var result = monitoringApi.JobDetails("579e47b79d01c5191c376260");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void JobDetails_ReturnsResult_WhenJobExists()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var job1 = CreateJobInState(database, null, EnqueuedState.StateName);

                var result = monitoringApi.JobDetails(job1.Id);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.True(result.CreatedAt > database.GetServerTimeUtc().AddMinutes(-1));
                Assert.True(result.CreatedAt < database.GetServerTimeUtc().AddMinutes(1));
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, null, EnqueuedState.StateName);
                
                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsEmpty_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, null, FetchedStateName);
                
                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void EnqueuedJobs_ReturnsUnfetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, null, EnqueuedState.StateName);
                var unfetchedJob2 = CreateJobInState(database, null, EnqueuedState.StateName);
                var fetchedJob = CreateJobInState(database, null, FetchedStateName);
                
                var resultList = monitoringApi.EnqueuedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenThereIsNoJobs()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsSingleJob_WhenOneJobExistsThatIsFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, null, FetchedStateName);
                
                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(1, resultList.Count);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsEmpty_WhenOneJobExistsThatIsNotFetched()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var unfetchedJob = CreateJobInState(database, null, EnqueuedState.StateName);
                
                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Empty(resultList);
            });
        }

        [Fact, CleanDatabase]
        public void FetchedJobs_ReturnsFetchedJobsOnly_WhenMultipleJobsExistsInFetchedAndUnfetchedStates()
        {
            UseMonitoringApi((database, monitoringApi) =>
            {
                var fetchedJob = CreateJobInState(database, null, FetchedStateName);
                var fetchedJob2 = CreateJobInState(database, null, FetchedStateName);
                var unfetchedJob = CreateJobInState(database, null, EnqueuedState.StateName);
                
                var resultList = monitoringApi.FetchedJobs(DefaultQueue, From, PerPage);

                Assert.Equal(2, resultList.Count);
            });
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }

        private void UseMonitoringApi(Action<HangfireDbContext, MongoMonitoringApi> action)
        {
            using (var storage = ConnectionUtils.CreateStorage())
            {
                var monitoringApi = new MongoMonitoringApi(storage);
                action(storage.Connection, monitoringApi);
            }
        }

        private JobDto CreateJobInState(HangfireDbContext database, string jobId, string stateName)
        {
            var job = Job.FromExpression(() => SampleMethod("wrong"));
            
            if (jobId == null)
                jobId = ObjectId.GenerateNewId().ToString();

            var createdAt = database.GetServerTimeUtc();

            var jobState = new StateDto
            {
                Name = stateName,
                CreatedAt = createdAt,
                Data = stateName == EnqueuedState.StateName ? $"{{ 'EnqueuedAt': '{createdAt:o}' }}" : "{}",
                JobId = jobId,
            };
            database.State.InsertOne(jobState);

            var jobDto = new JobDto
            {
                Id = jobId,
                InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                Arguments = "['\\\"Arguments\\\"']",
                StateName = stateName,
                CreatedAt = createdAt,
                StateId = jobState.Id,
                Queue = DefaultQueue,
                FetchedAt = stateName == FetchedStateName ? (DateTime?)createdAt : null
            };
            database.Job.InsertOne(jobDto);
            
            return jobDto;
        }
    }
}
