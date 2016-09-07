using System;
using Hangfire.Mongo.MongoUtils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class JobDto
    {
        [BsonId(IdGenerator = typeof(AutoIncrementIntIdGenerator))]
        public int Id { get; set; }

        [BsonIgnoreIfDefault]
        public ObjectId StateId { get; set; }

        [BsonIgnoreIfNull]
        public string StateName { get; set; }

        [BsonRequired]
        public string InvocationData { get; set; }

        [BsonIgnoreIfNull]
        public string Arguments { get; set; }

        [BsonRequired]
        public DateTime CreatedAt { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? ExpireAt { get; set; }
    }
}