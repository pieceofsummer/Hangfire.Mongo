using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class StateDto
    {
        [BsonId]
        public ObjectId Id { get; set; }

        [BsonRequired]
        public int JobId { get; set; }

        [BsonRequired]
        public string Name { get; set; }

        [BsonIgnoreIfNull]
        public string Reason { get; set; }

        [BsonRequired]
        public DateTime CreatedAt { get; set; }

        [BsonIgnoreIfNull]
        public string Data { get; set; }
    }
}