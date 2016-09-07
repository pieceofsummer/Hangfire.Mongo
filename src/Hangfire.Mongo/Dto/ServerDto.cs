using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class ServerDto
    {
        [BsonId]
        public string Id { get; set; }

        [BsonIgnoreIfNull]
        public string Data { get; set; }

        [BsonRequired]
        public DateTime LastHeartbeat { get; set; }
    }
}