using System;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue.Mongo;
using Hangfire.Mongo.Tests.Utils;
using Xunit;

namespace Hangfire.Mongo.Tests.PersistentJobQueue.Mongo
{
    [Collection("Database")]
    public class MongoJobQueueMonitoringApiFacts
    {
        private const string QueueName1 = "queueName1";
        private const string QueueName2 = "queueName2";

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoJobQueueMonitoringApi(null));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnEmpty_WhenNoQueuesExist()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var queues = mongoJobQueueMonitoringApi.GetQueues();

                Assert.Empty(queues);
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnOneQueue_WhenOneQueueExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                CreateJobQueueDto(connection, QueueName1, false);

                var queues = mongoJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(1, queues.Count);
                Assert.Equal(QueueName1, queues.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetQueues_ShouldReturnTwoUniqueQueues_WhenThreeNonUniqueQueuesExist()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName2, false);

                var queues = mongoJobQueueMonitoringApi.GetQueues().ToList();

                Assert.Equal(2, queues.Count);
                Assert.True(queues.Contains(QueueName1));
                Assert.True(queues.Contains(QueueName2));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnEmpty_WhenOneJobWithAFetchedStateExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueueDto.JobId, enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto3 = CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto3.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName2, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, false);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, false);
                CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WheNoQueuesExist()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10);

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnEmpty_WhenOneJobWithNonFetchedStateExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                CreateJobQueueDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueueDto.JobId, enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto3 = CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto3.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                CreateJobQueueDto(connection, QueueName2, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobQueueDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobQueueDto(connection, QueueName1, true);
                CreateJobQueueDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.JobId));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.JobId));
            });
        }

        private static JobQueueDto CreateJobQueueDto(HangfireDbContext connection, string queue, bool isFetched)
        {
            var serverTime = connection.GetServerTimeUtc();

            var state = new StateDto()
            {
                CreatedAt = serverTime
            };
            connection.State.InsertOne(state);
            
            var job = new JobDto
            {
                CreatedAt = serverTime,
                StateId = state.Id,
                StateName = state.Name
            };
            connection.Job.InsertOne(job);

            var jobQueue = new JobQueueDto
            {
                Queue = queue,
                JobId = job.Id,
                FetchedAt = isFetched ? (DateTime?)serverTime.AddDays(-1) : null
            };
            connection.JobQueue.InsertOne(jobQueue);

            return jobQueue;
        }
        
        private static void UseConnection(Action<HangfireDbContext, MongoJobQueueMonitoringApi> action)
        {
            using (var storage = ConnectionUtils.CreateStorage())
            {
                var monitoringApi = new MongoJobQueueMonitoringApi(storage);
                action(storage.Connection, monitoringApi);
            }
        }
    }
}
