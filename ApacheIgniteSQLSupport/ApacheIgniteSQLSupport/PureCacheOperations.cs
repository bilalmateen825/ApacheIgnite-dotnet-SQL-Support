using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Query;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheIgniteSQLSupport
{
    public class PureCacheOperations
    {
        IIgnite m_ignite;

        public PureCacheOperations()
        {
        }

        public void PerformAction()
        {
            IIgnite ignite = Ignition.Start(GetIgniteConfigurations());

            ICache<int, Person> cache = ignite.GetOrCreateCache<int, Person>("persons");
            cache[1] = new Person { Name = "John Doe", Age = 27 };
            cache[2] = new Person { Name = "Jane Moe", Age = 43 };

            var scanQuery = new ScanQuery<int, Person>(new PersonFilter());
            IQueryCursor<ICacheEntry<int, Person>> queryCursor = cache.Query(scanQuery);

            foreach (ICacheEntry<int, Person> cacheEntry in queryCursor)
                Console.WriteLine(cacheEntry);

        }

        IgniteConfiguration GetIgniteConfigurations()
        {
            return new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person),
                      typeof(PersonFilter))
            }
            ;
        }
    }


    class PersonFilter : ICacheEntryFilter<int, Person>
    {
        public bool Invoke(ICacheEntry<int, Person> entry)
        {
            return entry.Value.Age > 30;
        }
    }
}
