using System;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class AggregatedCounterDto
    {
        [BsonId]
        public string Key { get; set; }

        [BsonRequired]
        public long Value { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? ExpireAt { get; set; }
    }
}