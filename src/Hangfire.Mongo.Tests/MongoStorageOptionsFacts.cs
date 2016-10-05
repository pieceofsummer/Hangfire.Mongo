﻿using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoStorageOptionsFacts
    {
        [Fact]
        public void Ctor_SetsTheDefaultOptions()
        {
            MongoStorageOptions options = new MongoStorageOptions();

            Assert.Equal("hangfire", options.Prefix);
            Assert.True(options.InvisibilityTimeout > TimeSpan.Zero);
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateClientId()
        {
            var options = new MongoStorageOptions();
            Assert.False(string.IsNullOrWhiteSpace(options.ClientId));
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateUniqueClientId()
        {
            var options1 = new MongoStorageOptions();
            var options2 = new MongoStorageOptions();
            var options3 = new MongoStorageOptions();

            IEnumerable<string> result = new[] { options1.ClientId, options2.ClientId, options3.ClientId }.Distinct();

            Assert.Equal(3, result.Count());
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsEqualToZero()
        {
            var options = new MongoStorageOptions();
            Assert.Throws<ArgumentException>("value",
                () => options.QueuePollInterval = TimeSpan.Zero);
        }

        [Fact]
        public void Set_QueuePollInterval_ShouldThrowAnException_WhenGivenIntervalIsNegative()
        {
            var options = new MongoStorageOptions();
            Assert.Throws<ArgumentException>("value",
                () => options.QueuePollInterval = TimeSpan.FromSeconds(-1));
        }

        [Fact]
        public void Set_QueuePollInterval_SetsTheValue()
        {
            var options = new MongoStorageOptions
            {
                QueuePollInterval = TimeSpan.FromSeconds(1)
            };
            Assert.Equal(TimeSpan.FromSeconds(1), options.QueuePollInterval);
        }
    }
}