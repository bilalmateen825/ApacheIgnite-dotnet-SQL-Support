using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Affinity;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheIgniteSQLSupport
{
    public class SQLVersion
    {
        IIgnite m_ignite;
        public void PerformAction()
        {
            var cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person), typeof(Organization))
            };

            m_ignite = Ignition.Start(cfg);

            ICache<int, Person> personCache = m_ignite.GetOrCreateCache<int, Person>(
                new CacheConfiguration("persons", typeof(Person)));

            var orgCache = m_ignite.GetOrCreateCache<int, Organization>(
                new CacheConfiguration("orgs", typeof(Organization)));

            personCache[1] = new Person { Name = "John Doe", Age = 27, OrgId = 1 };
            personCache[2] = new Person { Name = "Jane Moe", Age = 43, OrgId = 2 };
            personCache[3] = new Person { Name = "Ivan Petrov", Age = 59, OrgId = 2 };

            orgCache[1] = new Organization { Id = 1, Name = "Contoso" };
            orgCache[2] = new Organization { Id = 2, Name = "Apache" };

            var fieldsQuery = new SqlFieldsQuery(
                "select Person.Name from Person " +
                "join \"orgs\".Organization as org on (Person.OrgId = org.Id) " +
                "where org.Name = ?", "Apache");

            foreach (var fieldList in personCache.QueryFields(fieldsQuery))
                Console.WriteLine(fieldList[0]);  // Jane Moe, Ivan Petrov

            var scanQuery = new ScanQuery<int, Person>(new PersonFilter());
            IQueryCursor<ICacheEntry<int, Person>> queryCursor = personCache.Query(scanQuery);

            foreach (ICacheEntry<int, Person> cacheEntry in queryCursor)
                Console.WriteLine(cacheEntry);
        }

        public void PerformActionWithAffinity()
        {
            var personCfg = new CacheConfiguration("persons")
            {
                QueryEntities = new[]
                {
                    new QueryEntity(typeof(PersonKey), typeof(Person)) { TableName = "Person" }
                }
            };

            var orgCfg = new CacheConfiguration("orgs")
            {
                QueryEntities = new[]
                {
                    new QueryEntity(typeof(int), typeof(Organization)) { TableName = "Organization" }
                }
            };

            var cfg = new IgniteConfiguration
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Person), typeof(Organization))
            };

            m_ignite = Ignition.Start(cfg);

            var personCache = m_ignite.GetOrCreateCache<PersonKey, Person>(personCfg);
            var orgCache = m_ignite.GetOrCreateCache<int, Organization>(orgCfg);

            orgCache[1] = new Organization { Id = 1, Name = "Contoso" };
            orgCache[2] = new Organization { Id = 2, Name = "Apache" };

            personCache[new PersonKey { Id = 1, OrgId = 1 }] = new Person { Name = "John Doe", Age = 27, OrgId = 1 };
            personCache[new PersonKey { Id = 2, OrgId = 2 }] = new Person { Name = "Jane Moe", Age = 43, OrgId = 2 };
            personCache[new PersonKey { Id = 3, OrgId = 2 }] = new Person { Name = "Ivan Petrov", Age = 59, OrgId = 2 };

            var q = new SqlFieldsQuery(
                        "SELECT p.Name " +
                        "FROM Person p " +
                        "JOIN \"orgs\".Organization o ON p.OrgId = o._key " +
                        "WHERE o.Name = ?",
                        "Apache")
            {
                Schema = "persons"
            };

            foreach (var row in personCache.QueryFields(q))
                Console.WriteLine(row[0]); // Jane Moe, Ivan Petrov

        }
    }


    public class PersonKey
    {
        [QuerySqlField]               // expose to SQL if you want SELECT/WHERE by Id
        public int Id { get; set; }

        [AffinityKeyMapped]
        [QuerySqlField(IsIndexed = true)]  // join/filter on OrgId → index it
        public int OrgId { get; set; }
    }

    class Person
    {
        [QuerySqlField]
        public string Name { get; set; }

        [QuerySqlField]
        public int Age { get; set; }

        [QuerySqlField]
        public int OrgId { get; set; }
    }

    class Organization
    {
        [QuerySqlField(IsIndexed = true)]
        public string Name { get; set; }

        [QuerySqlField]
        public int Id { get; set; }
    }
}
