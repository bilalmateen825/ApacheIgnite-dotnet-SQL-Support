// See https://aka.ms/new-console-template for more information
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Query;
using ApacheIgniteSQLSupport;
using System;

ENIgniteUsage eNIgniteUsage = ENIgniteUsage.SQLVersion;

Console.WriteLine("Hello, World!");

if (eNIgniteUsage == ENIgniteUsage.SQLVersion)
{

    SQLVersion sQLVersion = new SQLVersion();
    sQLVersion.PerformActionWithAffinity();
}
else if (eNIgniteUsage == ENIgniteUsage.PureCacheOperations)
{
    PureCacheOperations pureCacheOperations = new PureCacheOperations();
    pureCacheOperations.PerformAction();
}


Console.ReadLine();


public class Person
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public int OrgId { get; set; }
    public override string ToString() => $"Person[Id={Id}, Name={Name}, Age={Age}, OrgId={OrgId}]";
}

public class Organization
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public override string ToString() => $"Organization[Id={Id}, Name={Name}]";
}

public enum ENIgniteUsage
{
    SQLVersion,
    PureCacheOperations,
}
