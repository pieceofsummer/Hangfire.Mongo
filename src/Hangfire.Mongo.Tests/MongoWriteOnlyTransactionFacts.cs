using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.States;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoWriteOnlyTransactionFacts
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public MongoWriteOnlyTransactionFacts()
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            defaultProvider.Setup(x => x.GetJobQueue())
                .Returns(new Mock<IPersistentJobQueue>().Object);

            _queueProviders = new PersistentJobQueueProviderCollection(defaultProvider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfConnectionIsNull()
        {
            Assert.Throws<ArgumentNullException>("connection",
                () => new MongoWriteOnlyTransaction(null, _queueProviders));
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_IfProvidersCollectionIsNull()
        {
            Assert.Throws<ArgumentNullException>("queueProviders",
                () => new MongoWriteOnlyTransaction(ConnectionUtils.CreateConnection(), null));
        }

        [Fact, CleanDatabase]
        public void ExpireJob_SetsJobExpirationData()
        {
            UseConnection(database =>
            {
                JobDto job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(job);

                JobDto anotherJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(anotherJob);

                var jobId = job.Id;
                var anotherJobId = anotherJob.Id;

                Commit(database, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

                var testJob = GetTestJob(database, jobId);
                Assert.True(testJob.ExpireAt > database.GetServerTimeUtc().AddDays(1).AddMinutes(-1));
                Assert.True(testJob.ExpireAt < database.GetServerTimeUtc().AddDays(1).AddMinutes(1));

                var anotherTestJob = GetTestJob(database, anotherJobId);
                Assert.Null(anotherTestJob.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            UseConnection(database =>
            {
                JobDto job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = database.GetServerTimeUtc(),
                    ExpireAt = database.GetServerTimeUtc()
                };
                database.Job.InsertOne(job);

                JobDto anotherJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = database.GetServerTimeUtc(),
                    ExpireAt = database.GetServerTimeUtc()
                };
                database.Job.InsertOne(anotherJob);

                var jobId = job.Id;
                var anotherJobId = anotherJob.Id;

                Commit(database, x => x.PersistJob(jobId.ToString()));

                var testjob = GetTestJob(database, jobId);
                Assert.Null(testjob.ExpireAt);

                var anotherTestJob = GetTestJob(database, anotherJobId);
                Assert.NotNull(anotherTestJob.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            UseConnection(database =>
            {
                JobDto job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(job);

                JobDto anotherJob = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(anotherJob);

                var jobId = job.Id;
                var anotherJobId = anotherJob.Id;

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(database, x => x.SetJobState(jobId.ToString(), state.Object));

                var testJob = GetTestJob(database, jobId);
                Assert.Equal("State", testJob.StateName);
                Assert.NotNull(testJob.StateId);

                var anotherTestJob = GetTestJob(database, anotherJobId);
                Assert.Null(anotherTestJob.StateName);
                Assert.Equal(null, anotherTestJob.StateId);

                StateDto jobState = database.State.AsQueryable().Single();
                Assert.Equal(jobId, jobState.JobId);
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.NotNull(jobState.CreatedAt);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.Data);
            });
        }

        [Fact, CleanDatabase]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            UseConnection(database =>
            {
                JobDto job = new JobDto
                {
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.Job.InsertOne(job);

                var jobId = job.Id;

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(database, x => x.AddJobState(jobId.ToString(), state.Object));

                var testJob = GetTestJob(database, jobId);
                Assert.Null(testJob.StateName);
                Assert.Equal(null, testJob.StateId);

                StateDto jobState = database.State.AsQueryable().Single();
                Assert.Equal(jobId, jobState.JobId);
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.NotNull(jobState.CreatedAt);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.Data);
            });
        }

        [Fact, CleanDatabase]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            UseConnection(database =>
            {
                var correctJobQueue = new Mock<IPersistentJobQueue>();
                var correctProvider = new Mock<IPersistentJobQueueProvider>();
                correctProvider.Setup(x => x.GetJobQueue())
                    .Returns(correctJobQueue.Object);

                _queueProviders.Add(correctProvider.Object, new[] { "default" });

                Commit(database, x => x.AddToQueue("default", "579e47b79d01c5191c376260"));

                correctJobQueue.Verify(x => x.Enqueue("default", "579e47b79d01c5191c376260"));
                correctJobQueue.Verify(x => x.NotifyQueueChanged(), Times.Once);
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_NoExpiry_AddsRecord_WithPositiveValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.IncrementCounter("my-key"));

                CounterDto record = database.Counter.AsQueryable().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1, record.Value);
                Assert.Equal(null, record.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExpiry_AddsRecord_WithPositiveValueAndExpirationDate()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                CounterDto record = database.Counter.AsQueryable().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = (DateTime)record.ExpireAt;

                Assert.True(expireAt > database.GetServerTimeUtc().AddHours(23));
                Assert.True(expireAt < database.GetServerTimeUtc().AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_AddsNewRecord_ForEachOperation()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                var recordCount = database.Counter.AsQueryable().Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_NoExpiry_AddsRecord_WithNegativeValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.DecrementCounter("my-key"));

                CounterDto record = database.Counter.AsQueryable().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1, record.Value);
                Assert.Equal(null, record.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExpiry_AddsRecord_WithNegativeValueAndExpirationDate()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                CounterDto record = database.Counter.AsQueryable().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = (DateTime)record.ExpireAt;

                Assert.True(expireAt > database.GetServerTimeUtc().AddHours(23));
                Assert.True(expireAt < database.GetServerTimeUtc().AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_AddsNewRecord_ForEachOperation()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.DecrementCounter("my-key");
                    x.DecrementCounter("my-key");
                });

                var recordCount = database.Counter.AsQueryable().Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsNewRecord_ForNewKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.AddToSet("my-key", "my-value"));

                SetDto record = database.Set.AsQueryable().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(0.0, record.Score, 2);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsNewRecord_ForExistingKeyAndNewValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                var recordCount = database.Set.AsQueryable().Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_UpdatesRecord_ForExistingKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                var recordCount = database.Set.AsQueryable().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_AddsNewRecord_WithNewKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.AddToSet("my-key", "my-value", 3.2));

                SetDto record = database.Set.AsQueryable().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(3.2, record.Score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_UpdatesRecord_ForExistingKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                SetDto record = database.Set.AsQueryable().Single();

                Assert.Equal(3.2, record.Score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_RemovesRecord_ForGivenKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = database.Set.AsQueryable().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecords_WithSameKeyAndDifferentValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = database.Set.AsQueryable().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecords_WithDifferentKeyAndSameValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = database.Set.AsQueryable().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsNewRecord_ForNewKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.InsertToList("my-key", "my-value"));

                ListDto record = database.List.AsQueryable().Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsNewRecord_ForExistingKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_RemovesAllRecords_ForGivenKeyAndValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKeyAndDifferentValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithDifferentKeyAndSameValue()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_TrimsListToASpecifiedRange()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                ListDto[] records = database.List.AsQueryable().ToArray();

                Assert.Equal(2, records.Length);
                Assert.Equal("1", records[0].Value);
                Assert.Equal("2", records[1].Value);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecordsToEnd_IfKeepEndingAtIsGreaterThanItemsCountMinusOne()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfKeepStartingFromIsGreaterThanItemsCountMinusOne()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfKeepStartingFromIsGreaterThanKeepEndingAt()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecords_OnlyForGivenKey()
        {
            UseConnection(database =>
            {
                Commit(database, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = database.List.AsQueryable().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => Commit(database, x => x.SetRangeInHash(null, new Dictionary<string, string>())));
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>("keyValuePairs",
                    () => Commit(database, x => x.SetRangeInHash("some-hash", null)));
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection(database =>
            {
                Commit(database, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                var result = database.Hash.AsQueryable()
                    .Where(_ => _.Key == "some-hash")
                    .ToDictionary(_ => _.Field, _ => _.Value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(database =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => Commit(database, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_RemovesAllHashRecords()
        {
            UseConnection(database =>
            {
                // Arrange
                Commit(database, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                // Act
                Commit(database, x => x.RemoveHash("some-hash"));

                // Assert
                var count = database.Hash.AsQueryable().Count();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_SetsSetExpirationData()
        {
            UseConnection(database =>
            {
                var set1 = new SetDto { Key = "Set1", Value = "value1" };
                database.Set.InsertOne(set1);

                var set2 = new SetDto { Key = "Set2", Value = "value2" };
                database.Set.InsertOne(set2);

                Commit(database, x => x.ExpireSet(set1.Key, TimeSpan.FromDays(1)));

                var testSet1 = GetTestSet(database, set1.Key).FirstOrDefault();
                Assert.True(testSet1.ExpireAt > database.GetServerTimeUtc().AddHours(23));
                Assert.True(testSet1.ExpireAt < database.GetServerTimeUtc().AddHours(25));

                var testSet2 = GetTestSet(database, set2.Key).FirstOrDefault();
                Assert.NotNull(testSet2);
                Assert.Null(testSet2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_SetsListExpirationData()
        {
            UseConnection(database =>
            {
                var list1 = new ListDto { Key = "List1", Value = "value1" };
                database.List.InsertOne(list1);

                var list2 = new ListDto { Key = "List2", Value = "value2" };
                database.List.InsertOne(list2);

                Commit(database, x => x.ExpireList(list1.Key, TimeSpan.FromDays(1)));

                var testList1 = GetTestList(database, list1.Key);
                Assert.True(testList1.ExpireAt > database.GetServerTimeUtc().AddHours(23));
                Assert.True(testList1.ExpireAt < database.GetServerTimeUtc().AddHours(25));

                var testList2 = GetTestList(database, list2.Key);
                Assert.Null(testList2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_SetsHashExpirationData()
        {
            UseConnection(database =>
            {
                var hash1 = new HashDto { Key = "Hash1", Value = "value1" };
                database.Hash.InsertOne(hash1);

                var hash2 = new HashDto { Key = "Hash2", Value = "value2" };
                database.Hash.InsertOne(hash2);

                Commit(database, x => x.ExpireHash(hash1.Key, TimeSpan.FromDays(1)));

                var testHash1 = GetTestHash(database, hash1.Key);
                Assert.True(testHash1.ExpireAt > database.GetServerTimeUtc().AddHours(23));
                Assert.True(testHash1.ExpireAt < database.GetServerTimeUtc().AddHours(25));

                var testHash2 = GetTestHash(database, hash2.Key);
                Assert.Null(testHash2.ExpireAt);
            });
        }
        
        [Fact, CleanDatabase]
        public void PersistSet_ClearsTheSetExpirationData()
        {
            UseConnection(database =>
            {
                var set1 = new SetDto { Key = "Set1", Value = "value1", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set1);

                var set2 = new SetDto { Key = "Set2", Value = "value2", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set2);

                Commit(database, x => x.PersistSet(set1.Key));

                var testSet1 = GetTestSet(database, set1.Key).First();
                Assert.Null(testSet1.ExpireAt);

                var testSet2 = GetTestSet(database, set2.Key).First();
                Assert.NotNull(testSet2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ClearsTheListExpirationData()
        {
            UseConnection(database =>
            {
                var list1 = new ListDto { Key = "List1", Value = "value1", ExpireAt = database.GetServerTimeUtc() };
                database.List.InsertOne(list1);

                var list2 = new ListDto { Key = "List2", Value = "value2", ExpireAt = database.GetServerTimeUtc() };
                database.List.InsertOne(list2);

                Commit(database, x => x.PersistList(list1.Key));

                var testList1 = GetTestList(database, list1.Key);
                Assert.Null(testList1.ExpireAt);

                var testList2 = GetTestList(database, list2.Key);
                Assert.NotNull(testList2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ClearsTheHashExpirationData()
        {
            UseConnection(database =>
            {
                var hash1 = new HashDto { Key = "Hash1", Value = "value1", ExpireAt = database.GetServerTimeUtc() };
                database.Hash.InsertOne(hash1);

                var hash2 = new HashDto { Key = "Hash2", Value = "value2", ExpireAt = database.GetServerTimeUtc() };
                database.Hash.InsertOne(hash2);

                Commit(database, x => x.PersistHash(hash1.Key));

                var testHash1 = GetTestHash(database, hash1.Key);
                Assert.Null(testHash1.ExpireAt);

                var testHash2 = GetTestHash(database, hash2.Key);
                Assert.NotNull(testHash2.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_AddToExistingSetData()
        {
            UseConnection(database =>
            {
                var set1Val1 = new SetDto { Key = "Set1", Value = "value1", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set1Val1);

                var set1Val2 = new SetDto { Key = "Set1", Value = "value2", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set1Val2);

                var set2 = new SetDto { Key = "Set2", Value = "value2", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set2);

                var values = new[] { "test1", "test2", "test3" };
                Commit(database, x => x.AddRangeToSet(set1Val1.Key, values));

                var testSet1 = GetTestSet(database, set1Val1.Key);
                Assert.NotNull(testSet1);
                Assert.Equal(5, testSet1.Count);

                var testSet2 = GetTestSet(database, set2.Key);
                Assert.NotNull(testSet2);
                Assert.Equal(1, testSet2.Count);
            });
        }
        
        [Fact, CleanDatabase]
        public void RemoveSet_ClearsTheSetData()
        {
            UseConnection(database =>
            {
                var set1Val1 = new SetDto { Key = "Set1", Value = "value1", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set1Val1);

                var set1Val2 = new SetDto { Key = "Set1", Value = "value2", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set1Val2);

                var set2 = new SetDto { Key = "Set2", Value = "value2", ExpireAt = database.GetServerTimeUtc() };
                database.Set.InsertOne(set2);

                Commit(database, x => x.RemoveSet(set1Val1.Key));

                var testSet1 = GetTestSet(database, set1Val1.Key);
                Assert.Equal(0, testSet1.Count);

                var testSet2 = GetTestSet(database, set2.Key);
                Assert.Equal(1, testSet2.Count);
            });
        }
        
        private static JobDto GetTestJob(HangfireDbContext database, string jobId)
        {
            return database.Job.AsQueryable().FirstOrDefault(_ => _.Id == jobId);
        }

        private static IList<SetDto> GetTestSet(HangfireDbContext database, string key)
        {
            return database.Set.AsQueryable().Where(_ => _.Key == key).ToList();
        }

        private static ListDto GetTestList(HangfireDbContext database, string key)
        {
            return database.List.AsQueryable().FirstOrDefault(_ => _.Key == key);
        }

        private static HashDto GetTestHash(HangfireDbContext database, string key)
        {
            return database.Hash.AsQueryable().FirstOrDefault(_ => _.Key == key);
        }

        private void UseConnection(Action<HangfireDbContext> action)
        {
            using (HangfireDbContext connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
        
        private void Commit(HangfireDbContext connection, Action<MongoWriteOnlyTransaction> action)
        {
            using (var transaction = new MongoWriteOnlyTransaction(connection, _queueProviders))
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
}