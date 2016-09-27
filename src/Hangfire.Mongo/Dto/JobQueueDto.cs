using System;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class JobQueueDto
    {
        [BsonId, BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRequired, BsonRepresentation(BsonType.ObjectId)]
        public string JobId { get; set; }

        [BsonRequired]
        public string Queue { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? FetchedAt { get; set; }
    }
}