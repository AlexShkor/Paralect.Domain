using System;
using System.Linq;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Paralect.Transitions.Mongo
{
    public class MongoTransitionRepository : ITransitionRepository
    {
        private const string _concurrencyException = "E1100";
        private readonly IDataTypeRegistry _dataTypeRegistry;
        private readonly MongoTransitionServer _server;
        private readonly MongoTransitionSerializer _serializer;

        public MongoTransitionRepository(IDataTypeRegistry dataTypeRegistry, String connectionString, String collectionName = "transitions")
        {
            _dataTypeRegistry = dataTypeRegistry;
            _serializer = new MongoTransitionSerializer(dataTypeRegistry);
            _server = new MongoTransitionServer(connectionString, collectionName);

            EnsureIndexes();
        }

        private Dictionary<BsonDocument, IndexKeysBuilder> RequiredIndexes()
        {
            return new Dictionary<BsonDocument, IndexKeysBuilder>()
            {
                {new BsonDocument("_id.StreamId", 1), IndexKeys.Ascending("_id.StreamId")},
                {new BsonDocument("_id.Version", 1), IndexKeys.Ascending("_id.Version")},
                {new BsonDocument("Timestamp", 1), IndexKeys.Ascending("Timestamp")},
                {new BsonDocument()
                     {
                         new BsonElement("Timestamp", 1),
                         new BsonElement("_id.Version", 1),
                         
                     }, IndexKeys.Ascending("Timestamp", "_id.Version")},
            };
        }

        public void EnsureIndexes()
        {
            var indexes = _server.Transitions.GetIndexes().Select(x => x.Key as BsonDocument).ToList();
            foreach (var index in RequiredIndexes())
            {
                if (!indexes.Contains(index.Key))
                    _server.Transitions.CreateIndex(index.Value);
            }
        }

        public void SaveTransition(Transition transition)
        {
            // skip saving empty transition
            if (transition.Events.Count < 1)
                return;

            var doc = _serializer.Serialize(transition);

            try
            {
                _server.Transitions.Insert(doc, SafeMode.True);
            }
            catch (MongoException e)
            {
                if (!e.Message.Contains(_concurrencyException))
                    throw;

                throw new DuplicateTransitionException(transition.Id.StreamId, transition.Id.Version, e);
            }
        }

        public List<Transition> GetTransitions(string streamId, int fromVersion, int toVersion)
        {
            var query = Query.And(Query.EQ("_id.StreamId", streamId),
                                  Query.GTE("_id.Version", fromVersion).LTE(toVersion));

            var sort = SortBy.Ascending("_id.Version");

            var docs = _server.Transitions.FindAs<BsonDocument>(query)
                .SetSortOrder(sort)
                .ToList();

            // Check that such stream exists
            if (docs.Count < 1)
                throw new ArgumentException(String.Format("There is no stream in store with id {0}", streamId));

            var transitions = docs.Select(_serializer.Deserialize).ToList();

            return transitions;
        }

        /// <summary>
        /// Get all transitions ordered ascendantly by Timestamp of transiton
        /// Should be used only for testing and for very simple event replying 
        /// </summary>
        public List<Transition> GetTransitions()
        {
            var docs = _server.Transitions.FindAllAs<BsonDocument>()
                .SetSortOrder(SortBy.Ascending("Timestamp", "_id.Version"))
                .ToList();

            var transitions = docs.Select(_serializer.Deserialize).ToList();

            return transitions;
        }

        public void RemoveTransition(string streamId, int version)
        {
            var id = _serializer.SerializeTransitionId(new TransitionId(streamId, version));
            var query = new BsonDocument { { "_id", id } };

            _server.Transitions.Remove(new QueryDocument(query));
        }

        public void RemoveStream(String streamId)
        {
            var query = new BsonDocument { { "_id.StreamId", streamId } };
            _server.Transitions.Remove(new QueryDocument(query));
        }
    }
}
