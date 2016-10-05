using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoConnectionFacts
    {
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly Mock<IPersistentJobQueueProvider> _provider;
        private readonly PersistentJobQueueProviderCollection _providers;

        public MongoConnectionFacts()
        {
            _queue = new Mock<IPersistentJobQueue>();

            _provider = new Mock<IPersistentJobQueueProvider>();
            _provider.Setup(x => x.GetJobQueue()).Returns(_queue.Object);

            _providers = new PersistentJobQueueProviderCollection(_provider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoConnection(null, _providers));

            Assert.Equal("database", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoConnection(ConnectionUtils.CreateConnection(), null));

            Assert.Equal("queueProviders", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            UseConnection((database, connection) =>
            {
                var token = new CancellationToken();
                var queues = new[] { "default" };

                connection.FetchNextJob(queues, token);

                _queue.Verify(x => x.Dequeue(queues, token));
            });
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_Throws_IfMultipleProvidersResolved()
        {
            UseConnection((database, connection) =>
            {
                var token = new CancellationToken();

                var anotherProvider = new Mock<IPersistentJobQueueProvider>();
                _providers.Add(anotherProvider.Object, new[] { "critical" });

                Assert.Throws<InvalidOperationException>(
                    () => connection.FetchNextJob(new[] { "critical", "default" }, token));
            });
        }

        [Fact, CleanDatabase]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            UseConnection((database, connection) =>
            {
                using (var transaction = connection.CreateWriteTransaction())
                {
                    Assert.NotNull(transaction);
                }
            });
        }

        [Fact, CleanDatabase]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            UseConnection((database, connection) =>
            {
                using (var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1)))
                {
                    Assert.NotNull(@lock);
                }
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("job",
                    () => connection.CreateExpiredJob(null, new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.Zero));
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("parameters",
                    () => connection.CreateExpiredJob(Job.FromExpression(() => SampleMethod("hello")), 
                                                      null, DateTime.UtcNow, TimeSpan.Zero));
            });
        }

        [Fact, CleanDatabase]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            UseConnection((database, connection) =>
            {
                var createdAt = new DateTime(2012, 12, 12, 0, 0, 0, 0, DateTimeKind.Utc);
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => SampleMethod("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);

                var databaseJob = database.Job.AsQueryable().Single();
                Assert.Equal(jobId, databaseJob.Id);
                Assert.Equal(createdAt, databaseJob.CreatedAt);
                Assert.Equal(null, databaseJob.StateId);
                Assert.Equal(null, databaseJob.StateName);

                var invocationData = JobHelper.FromJson<InvocationData>(databaseJob.InvocationData);
                invocationData.Arguments = databaseJob.Arguments;

                var job = invocationData.Deserialize();
                Assert.Equal(typeof(MongoConnectionFacts), job.Type);
                Assert.Equal(nameof(SampleMethod), job.Method.Name);
                Assert.Equal("Hello", job.Args[0]);

                Assert.True(databaseJob.ExpireAt > createdAt.AddDays(1).AddMinutes(-1));
                Assert.True(databaseJob.ExpireAt < createdAt.AddDays(1).AddMinutes(1));

                var parameters = database.JobParameter.AsQueryable()
                    .Where(_ => _.JobId == jobId)
                    .ToDictionary(_ => _.Name, _ => _.Value);

                Assert.Equal("Value1", parameters["Key1"]);
                Assert.Equal("Value2", parameters["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("jobId",
                    () => connection.GetJobData(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetJobData(ObjectId.GenerateNewId().ToString());
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            UseConnection((database, connection) =>
            {
                var job = Job.FromExpression(() => SampleMethod("wrong"));

                var jobDto = new JobDto
                {
                    InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                    Arguments = "['\\\"Arguments\\\"']",
                    StateName = "Succeeded",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(jobDto);

                var result = connection.GetJobData(jobDto.Id);

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal("Succeeded", result.State);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.Null(result.LoadException);
                Assert.True(result.CreatedAt > DateTime.UtcNow.AddMinutes(-1));
                Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("jobId",
                    () => connection.GetStateData(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetStateData(ObjectId.GenerateNewId().ToString());
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsCorrectData()
        {
            UseConnection((database, connection) =>
            {
                var data = new Dictionary<string, string>
                {
                    { "Key", "Value" }
                };

                var jobDto = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    StateName = "",
                    CreatedAt = DateTime.UtcNow
                };

                database.Job.InsertOne(jobDto);
                var jobId = jobDto.Id;

                database.State.InsertOne(new StateDto
                {
                    JobId = jobId,
                    Name = "old-state",
                    CreatedAt = DateTime.UtcNow
                });

                var stateDto = new StateDto
                {
                    JobId = jobId,
                    Name = "Name",
                    Reason = "Reason",
                    Data = JobHelper.ToJson(data),
                    CreatedAt = DateTime.UtcNow
                };
                database.State.InsertOne(stateDto);

                jobDto.StateId = stateDto.Id;
                database.Job.ReplaceOne(_ => _.Id == jobDto.Id, jobDto, new UpdateOptions());

                var result = connection.GetStateData(jobId);
                Assert.NotNull(result);

                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            UseConnection((database, connection) =>
            {
                var jobDto = new JobDto
                {
                    InvocationData = JobHelper.ToJson(new InvocationData(null, null, null, null)),
                    Arguments = "['\\\"Arguments\\\"']",
                    StateName = "Succeeded",
                    CreatedAt = database.GetServerTimeUtc()
                };
                database.Job.InsertOne(jobDto);

                var jobId = jobDto.Id;

                var result = connection.GetJobData(jobId);

                Assert.NotNull(result.LoadException);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("jobId",
                    () => connection.SetJobParameter(null, "name", "value"));
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("name",
                    () => connection.SetJobParameter("547527b4c6b6cc26a02d021d", null, "value"));
            });
        }

        [Fact, CleanDatabase]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            UseConnection((database, connection) =>
            {
                var jobDto = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(jobDto);
                var jobId = jobDto.Id;

                connection.SetJobParameter(jobId, "Name", "Value");

                var parameter = database.JobParameter.AsQueryable()
                    .Where(_ => _.JobId == jobId && _.Name == "Name")
                    .Single();

                Assert.Equal("Value", parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            UseConnection((database, connection) =>
            {
                var jobDto = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(jobDto);
                var jobId = jobDto.Id;

                connection.SetJobParameter(jobId, "Name", "Value");
                connection.SetJobParameter(jobId, "Name", "AnotherValue");

                var parameter = database.JobParameter.AsQueryable()
                    .Where(_ => _.JobId == jobId && _.Name == "Name")
                    .Single();

                Assert.Equal("AnotherValue", parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            UseConnection((database, connection) =>
            {
                var jobDto = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(jobDto);
                var jobId = jobDto.Id;

                connection.SetJobParameter(jobId, "Name", null);

                var parameter = database.JobParameter.AsQueryable()
                    .Where(_ => _.JobId == jobId && _.Name == "Name")
                    .Single();

                Assert.Equal(null, parameter.Value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("jobId",
                    () => connection.GetJobParameter(null, "hello"));
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("name",
                    () => connection.GetJobParameter("547527b4c6b6cc26a02d021d", null));
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            UseConnection((database, connection) =>
            {
                var value = connection.GetJobParameter("547527b4c6b6cc26a02d021d", "hello");
                Assert.Null(value);
            });
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            UseConnection((database, connection) =>
            {
                var jobDto = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(jobDto);
                var jobId = jobDto.Id;

                database.JobParameter.InsertOne(new JobParameterDto
                {
                    JobId = jobId,
                    Name = "name",
                    Value = "value"
                });

                var value = connection.GetJobParameter(jobId, "name");

                Assert.Equal("value", value);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetFirstByLowestScoreFromSet(null, 0, 1));
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            UseConnection((database, connection) => 
            {
                Assert.Throws<ArgumentException>(
                    () => connection.GetFirstByLowestScoreFromSet("key", 0, -1));
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetFirstByLowestScoreFromSet("key", 0, 1);
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            UseConnection((database, connection) =>
            {
                database.Set.InsertOne(new SetDto
                {
                    Key = "key",
                    Score = 1.0,
                    Value = "1.0"
                });
                database.Set.InsertOne(new SetDto
                {
                    Key = "key",
                    Score = -1.0,
                    Value = "-1.0"
                });
                database.Set.InsertOne(new SetDto
                {
                    Key = "key",
                    Score = -5.0,
                    Value = "-5.0"
                });
                database.Set.InsertOne(new SetDto
                {
                    Key = "another-key",
                    Score = -2.0,
                    Value = "-2.0"
                });

                var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

                Assert.Equal("-1.0", result);
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("serverId",
                    () => connection.AnnounceServer(null, new ServerContext()));
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("context",
                    () => connection.AnnounceServer("server", null));
            });
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            UseConnection((database, connection) =>
            {
                var context1 = new ServerContext
                {
                    Queues = new[] { "critical", "default" },
                    WorkerCount = 4
                };
                connection.AnnounceServer("server", context1);

                var server = database.Server.AsQueryable().Single();
                Assert.Equal("server", server.Id);
                Assert.StartsWith("{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":", server.Data);
                Assert.NotNull(server.LastHeartbeat);

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);

                var sameServer = database.Server.AsQueryable().Single();
                Assert.Equal("server", sameServer.Id);
                Assert.Contains("1000", sameServer.Data);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("serverId",
                    () => connection.RemoveServer(null));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveServer_RemovesAServerRecord()
        {
            UseConnection((database, connection) =>
            {
                database.Server.InsertOne(new ServerDto
                {
                    Id = "Server1",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow
                });
                database.Server.InsertOne(new ServerDto
                {
                    Id = "Server2",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow
                });

                connection.RemoveServer("Server1");

                var server = database.Server.AsQueryable().Single();
                Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
            });
        }

        [Fact, CleanDatabase]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("serverId",
                    () => connection.Heartbeat(null));
            });
        }

        [Fact, CleanDatabase]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            UseConnection((database, connection) =>
            {
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server1",
                    Data = "",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                });
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server2",
                    Data = "",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                });

                connection.Heartbeat("server1");

                var servers = database.Server.AsQueryable()
                    .ToDictionary(_ => _.Id, _ => _.LastHeartbeat);

                Assert.NotEqual(2012, servers["server1"].Year);
                Assert.Equal(2012, servers["server2"].Year);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentException>("timeOut",
                    () => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            UseConnection((database, connection) =>
            {
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server1",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow.AddDays(-1)
                });
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server2",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow.AddHours(-12)
                });

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                var liveServer = database.Server.AsQueryable().Single();
                Assert.Equal("server2", liveServer.Id);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetAllItemsFromSet(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Equal(0, result.Count);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.Set.InsertOne(new SetDto
                {
                    Key = "some-set",
                    Score = 0.0,
                    Value = "1"
                });
                database.Set.InsertOne(new SetDto
                {
                    Key = "some-set",
                    Score = 0.0,
                    Value = "2"
                });
                database.Set.InsertOne(new SetDto
                {
                    Key = "another-set",
                    Score = 0.0,
                    Value = "3"
                });

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("keyValuePairs",
                    () => connection.SetRangeInHash("some-hash", null));
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection((database, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                });

                var result = database.Hash.AsQueryable()
                    .Where(_ => _.Key == "some-hash")
                    .ToDictionary(_ => _.Field, _ => _.Value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetAllEntriesFromHash(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.Hash.InsertOne(new HashDto
                {
                    Key = "some-hash",
                    Field = "Key1",
                    Value = "Value1"
                });
                database.Hash.InsertOne(new HashDto
                {
                    Key = "some-hash",
                    Field = "Key2",
                    Value = "Value2"
                });
                database.Hash.InsertOne(new HashDto
                {
                    Key = "another-hash",
                    Field = "Key3",
                    Value = "Value3"
                });

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal(2, result.Count);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetSetCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetSetCount("my-set");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            UseConnection((database, connection) =>
            {
                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "value-1"
                });
                database.Set.InsertOne(new SetDto
                {
                    Key = "set-2",
                    Value = "value-1"
                });
                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "value-2"
                });

                var result = connection.GetSetCount("set-1");

                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetRangeFromSet(null, 0, 1));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ReturnsPagedElements()
        {
            UseConnection((database, connection) =>
            {
                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "1",
                    Score = 0.0
                });

                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "2",
                    Score = 0.0
                });

                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "3",
                    Score = 0.0
                });

                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "4",
                    Score = 0.0
                });

                database.Set.InsertOne(new SetDto
                {
                    Key = "set-2",
                    Value = "5",
                    Score = 0.0
                });

                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "6",
                    Score = 0.0
                });

                var result = connection.GetRangeFromSet("set-1", 2, 3);

                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetSetTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetSetTtl("my-set");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.Set.InsertOne(new SetDto
                {
                    Key = "set-1",
                    Value = "1",
                    Score = 0.0,
                    ExpireAt = DateTime.UtcNow.AddMinutes(60)
                });

                database.Set.InsertOne(new SetDto
                {
                    Key = "set-2",
                    Value = "2",
                    Score = 0.0,
                    ExpireAt = null
                });

                // Act
                var result = connection.GetSetTtl("set-1");

                // Assert
                Assert.True(result > TimeSpan.FromMinutes(59));
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetCounter(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetCounter("my-counter");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.Counter.InsertOne(new CounterDto
                {
                    Key = "counter-1",
                    Value = 1
                });
                database.Counter.InsertOne(new CounterDto
                {
                    Key = "counter-2",
                    Value = 1
                });
                database.Counter.InsertOne(new CounterDto
                {
                    Key = "counter-1",
                    Value = 1
                });

                // Act
                var result = connection.GetCounter("counter-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_IncludesValues_FromCounterAggregateTable()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.AggregatedCounter.InsertOne(new AggregatedCounterDto
                {
                    Key = "counter-1",
                    Value = 12
                });
                database.AggregatedCounter.InsertOne(new AggregatedCounterDto
                {
                    Key = "counter-2",
                    Value = 15
                });

                // Act
                var result = connection.GetCounter("counter-1");

                Assert.Equal(12, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetHashCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetHashCount("my-hash");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-1",
                    Field = "field-1"
                });
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-1",
                    Field = "field-2"
                });
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-2",
                    Field = "field-1"
                });

                // Act
                var result = connection.GetHashCount("hash-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetHashTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetHashTtl("my-hash");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-1",
                    Field = "field",
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-2",
                    Field = "field",
                    ExpireAt = null
                });

                // Act
                var result = connection.GetHashTtl("hash-1");

                // Assert
                Assert.True(result > TimeSpan.FromMinutes(59));
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetValueFromHash(null, "name"));
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("name",
                    () => connection.GetValueFromHash("key", null));
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetValueFromHash("my-hash", "name");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-1",
                    Field = "field-1",
                    Value = "1"
                });
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-1",
                    Field = "field-2",
                    Value = "2"
                });
                database.Hash.InsertOne(new HashDto
                {
                    Key = "hash-2",
                    Field = "field-1",
                    Value = "3"
                });

                // Act
                var result = connection.GetValueFromHash("hash-1", "field-1");

                // Assert
                Assert.Equal("1", result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetListCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetListCount("my-list");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-2",
                });

                // Act
                var result = connection.GetListCount("list-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetListTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetListTtl("my-list");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-2",
                    ExpireAt = null
                });

                // Act
                var result = connection.GetListTtl("list-1");

                // Assert
                Assert.True(result > TimeSpan.FromMinutes(59));
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetRangeFromList(null, 0, 1));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetRangeFromList("my-list", 0, 1);
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                    Value = "1"
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-2",
                    Value = "2"
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                    Value = "3"
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                    Value = "4"
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                    Value = "5"
                });

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 2);

                // Assert
                Assert.Equal(new[] { "3", "4" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetAllItemsFromList(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetAllItemsFromList("my-list");
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAllItems_FromAGivenList()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                    Value = "1"
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-2",
                    Value = "2"
                });
                database.List.InsertOne(new ListDto
                {
                    Key = "list-1",
                    Value = "3"
                });

                // Act
                var result = connection.GetAllItemsFromList("list-1");

                // Assert
                Assert.Equal(new[] { "1", "3" }, result);
            });
        }

        private void UseConnection(Action<HangfireDbContext, MongoConnection> action)
        {
            using (var database = ConnectionUtils.CreateConnection())
            {
                using (var connection = new MongoConnection(database, _providers))
                {
                    action(database, connection);
                }
            }
        }

        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }
    }
}