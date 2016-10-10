using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Hangfire.Mongo.Dto
{
    [BsonIgnoreExtraElements]
    internal class JobDto
    {
        [BsonId, BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRequired]
        public string InvocationData { get; set; }

        [BsonIgnoreIfNull]
        public string Arguments { get; set; }

        [BsonRequired]
        public DateTime CreatedAt { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? ExpireAt { get; set; }

        #region Job state

        [BsonIgnoreIfNull, BsonRepresentation(BsonType.ObjectId)]
        public string StateId { get; set; }

        [BsonIgnoreIfNull]
        public string StateName { get; set; }

        [BsonIgnoreIfNull]
        public string StateReason { get; set; }

        [BsonIgnoreIfNull]
        public string StateData { get; set; }

        #endregion

        #region Job queue

        [BsonIgnoreIfNull]
        public string Queue { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? FetchedAt { get; set; }

        #endregion
    }
}