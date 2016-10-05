using System;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    internal class JobDetailedDto : JobDto
    {
        [BsonIgnoreIfNull]
        public string StateReason { get; set; }

        [BsonIgnoreIfNull]
        public string StateData { get; set; }
    }
}