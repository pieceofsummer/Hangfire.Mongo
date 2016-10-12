using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Hangfire.Common;
using Hangfire.Storage;
using System.Collections.Generic;

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

        [BsonRequired, BsonRepresentation(BsonType.ObjectId)]
        public string StateId { get; set; }

        [BsonRequired]
        public string StateName { get; set; }

        [BsonIgnoreIfNull]
        public string StateReason { get; set; }

        [BsonIgnoreIfNull]
        public IDictionary<string, string> StateData { get; set; }

        #endregion

        #region Job queue

        [BsonIgnoreIfNull]
        public string Queue { get; set; }

        [BsonIgnoreIfNull]
        public DateTime? FetchedAt { get; set; }

        #endregion
    }
}