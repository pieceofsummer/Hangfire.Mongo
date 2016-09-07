using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class AggregatedCounterDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonRequired]
        public string Key { get; set; }

        [BsonRequired]
        public long Value { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? ExpireAt { get; set; }
    }
}