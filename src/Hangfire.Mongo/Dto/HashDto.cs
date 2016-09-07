using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class HashDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonRequired]
        public string Key { get; set; }

        [BsonRequired]
        public string Field { get; set; }

        public string Value { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? ExpireAt { get; set; }
    }
}