using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Collections.Generic;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class StateDto
    {
        [BsonId, BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRequired, BsonRepresentation(BsonType.ObjectId)]
        public string JobId { get; set; }

        [BsonRequired]
        public string Name { get; set; }

        [BsonIgnoreIfNull]
        public string Reason { get; set; }
        
        [BsonIgnoreIfNull]
        public IDictionary<string, string> Data { get; set; }

        [BsonRequired]
        public DateTime CreatedAt { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? ExpireAt { get; set; }
    }
}