using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class ServerDto
    {
        [BsonId]
        public string Name { get; set; }
        
        [BsonRequired]
        public DateTime Heartbeat { get; set; }
        
        [BsonRequired]
        public DateTime StartedAt { get; set; }

        [BsonRequired]
        public string[] Queues { get; set; }

        [BsonRequired]
        public int WorkerCount { get; set; }
    }
}