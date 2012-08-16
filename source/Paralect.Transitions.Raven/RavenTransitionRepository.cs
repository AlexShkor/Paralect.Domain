using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;

namespace Paralect.Transitions.Raven
{
    public class RavenTransitionRepository : ITransitionRepository
    {
        private readonly string _conntectionString;
        private readonly DocumentStore _documentStore;


        protected IDocumentSession Session
        {
            get { return _documentStore.Initialize().OpenSession(); }
        }

        public RavenTransitionRepository(string conntectionString)
        {
            _conntectionString = conntectionString;         
            _documentStore = new DocumentStore()
                                 {
                                     Url = conntectionString
                                 };
        }

        public void SaveTransition(Transition transition)
        {
            Session.Store(transition);
        }

        public List<Transition> GetTransitions(string streamId, int fromVersion, int toVersion)
        {
            return Session.Query<Transition>().Where(
                x => x.Id.StreamId == streamId && x.Id.Version >= fromVersion && x.Id.Version <= toVersion).ToList();
        }

        public List<Transition> GetTransitions(int startIndex, int count)
        {
            return
                Session.Query<Transition>().OrderBy(x => x.Timestamp).ThenBy(x => x.Id.Version).Skip(startIndex).Take(
                    count).ToList();
        }

        public long CountTransitions()
        {
            return Session.Query<Transition>().Count();
        }

        public List<Transition> GetTransitions()
        {
            return
                Session.Query<Transition>().OrderBy(x => x.Timestamp).ThenBy(x => x.Id.Version).ToList();
        }

        public void RemoveTransition(string streamId, int version)
        {
            var transitions =
                Session.Query<Transition>().Where(x => x.Id.StreamId == streamId && x.Id.Version == version);
            foreach (var transition in transitions)
            {
                Session.Delete(transition);
            }
        }

        public void RemoveStream(string streamId)
        {
            var transitions =
                Session.Query<Transition>().Where(x => x.Id.StreamId == streamId);
            foreach (var transition in transitions)
            {
                Session.Delete(transition);
            }
        }

        public void EnsureIndexes()
        {
            IndexCreation.CreateIndexes(typeof(RavenTransitionRepository).Assembly,_documentStore);
        }

        public class StreamIdIndex : AbstractIndexCreationTask<Transition>
        {
            public StreamIdIndex()
            {
                Map = documents => from document in documents
                                   select new
                                   {
                                       id_StreamId = document.Id.StreamId
                                   }; 
            }
        }

        public class VersionIndex : AbstractIndexCreationTask<Transition>
        {
            public VersionIndex()
            {
                Map = documents => from document in documents
                                   select new
                                   {
                                       id_Version = document.Id.Version
                                   };
            }
        }

        public class TimestampIndex : AbstractIndexCreationTask<Transition>
        {
            public TimestampIndex()
            {
                Map = documents => from document in documents
                                   select new
                                   {
                                       Timestamp = document.Timestamp
                                   };
            }
        }

        public class TimestampAndVersionIndex : AbstractIndexCreationTask<Transition>
        {
            public TimestampAndVersionIndex()
            {
                Map = documents => from document in documents
                                   select new
                                   {
                                       Timestamp = document.Timestamp,
                                       id_Version = document.Id.Version
                                   };
            }
        }
    }
}