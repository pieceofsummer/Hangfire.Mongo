using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class JobParameterDto
    {
        [BsonId, BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRequired, BsonRepresentation(BsonType.ObjectId)]
        public string JobId { get; set; }

        [BsonRequired]
        public string Name { get; set; }

        public string Value { get; set; }
        
        [BsonIgnoreIfNull]
        public DateTime? ExpireAt { get; set; }
    }
}