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
                CreateJobDto(connection, QueueName1, false);

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
                CreateJobDto(connection, QueueName1, false);
                CreateJobDto(connection, QueueName1, false);
                CreateJobDto(connection, QueueName2, false);

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
                CreateJobDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobDto = CreateJobDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobDto.Id, enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobDto = CreateJobDto(connection, QueueName1, false);
                var jobDto2 = CreateJobDto(connection, QueueName1, false);
                var jobDto3 = CreateJobDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobDto.Id));
                Assert.True(enqueuedJobIds.Contains(jobDto2.Id));
                Assert.True(enqueuedJobIds.Contains(jobDto3.Id));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobDto = CreateJobDto(connection, QueueName1, false);
                var jobDto2 = CreateJobDto(connection, QueueName1, false);
                CreateJobDto(connection, QueueName2, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobDto.Id));
                Assert.True(enqueuedJobIds.Contains(jobDto2.Id));
            });
        }

        [Fact, CleanDatabase]
        public void GetEnqueuedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobDto = CreateJobDto(connection, QueueName1, false);
                var jobDto2 = CreateJobDto(connection, QueueName1, false);
                CreateJobDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetEnqueuedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobDto.Id));
                Assert.True(enqueuedJobIds.Contains(jobDto2.Id));
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
                CreateJobDto(connection, QueueName1, false);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Empty(enqueuedJobIds);
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnOneJobId_WhenOneJobExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(1, enqueuedJobIds.Count);
                Assert.Equal(jobQueueDto.Id, enqueuedJobIds.First());
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnThreeJobIds_WhenThreeJobsExists()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobDto(connection, QueueName1, true);
                var jobQueueDto3 = CreateJobDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(3, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.Id));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.Id));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto3.Id));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButOnlyTwoInRequestedQueue()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobQueueDto = CreateJobDto(connection, QueueName1, true);
                var jobQueueDto2 = CreateJobDto(connection, QueueName1, true);
                CreateJobDto(connection, QueueName2, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 10).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobQueueDto.Id));
                Assert.True(enqueuedJobIds.Contains(jobQueueDto2.Id));
            });
        }

        [Fact, CleanDatabase]
        public void GetFetchedJobIds_ShouldReturnTwoJobIds_WhenThreeJobsExistsButLimitIsSet()
        {
            UseConnection((connection, mongoJobQueueMonitoringApi) =>
            {
                var jobDto = CreateJobDto(connection, QueueName1, true);
                var jobDto2 = CreateJobDto(connection, QueueName1, true);
                CreateJobDto(connection, QueueName1, true);

                var enqueuedJobIds = mongoJobQueueMonitoringApi.GetFetchedJobIds(QueueName1, 0, 2).ToList();

                Assert.Equal(2, enqueuedJobIds.Count);
                Assert.True(enqueuedJobIds.Contains(jobDto.Id));
                Assert.True(enqueuedJobIds.Contains(jobDto2.Id));
            });
        }

        private static JobDto CreateJobDto(HangfireDbContext connection, string queue, bool isFetched)
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
                StateName = state.Name,
                Queue = queue,
                FetchedAt = isFetched ? (DateTime?)serverTime.AddDays(-1) : null
            };
            connection.Job.InsertOne(job);
            
            return job;
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
