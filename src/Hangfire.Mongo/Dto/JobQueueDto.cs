using System;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class JobQueueDto
    {
        [BsonId(IdGenerator = typeof(AutoIncrementIntIdGenerator))]
        public int Id { get; set; }

        [BsonRequired]
        public int JobId { get; set; }

        [BsonRequired]
        public string Queue { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? FetchedAt { get; set; }
    }
}