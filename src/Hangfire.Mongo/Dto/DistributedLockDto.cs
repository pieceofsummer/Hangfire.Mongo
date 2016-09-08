using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class DistributedLockDto
    {
        [BsonId]
        public string Resource { get; set; }

        [BsonRequired]
        public DateTime ExpireAt { get; set; }

    }
}