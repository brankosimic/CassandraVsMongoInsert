using Cassandra;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraVsMongoInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            RunCassandra(100).GetAwaiter().GetResult();
            RunCassandra(1000).GetAwaiter().GetResult();
            RunCassandra(10000).GetAwaiter().GetResult();
            RunCassandra(100000).GetAwaiter().GetResult();
            RunCassandra(1000000).GetAwaiter().GetResult();
            
            RunMongo(100).GetAwaiter().GetResult();
            RunMongo(1000).GetAwaiter().GetResult();
            RunMongo(10000).GetAwaiter().GetResult();
            RunMongo(100000).GetAwaiter().GetResult();
            RunMongo(1000000).GetAwaiter().GetResult();
        }

        private static async Task RunCassandra(int usersNumber)
        {
            var connections = usersNumber / 2048;
            connections = connections > 0 ? connections : 1;

            PoolingOptions poolingOptions = new PoolingOptions();
            poolingOptions
                .SetCoreConnectionsPerHost(HostDistance.Local, connections)
                .SetMaxConnectionsPerHost(HostDistance.Local, connections);

            poolingOptions
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 32768);

            var cluster = Cluster.Builder()
                .AddContactPoints("localhost")
                .WithPoolingOptions(poolingOptions)
                .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
                .WithLoadBalancingPolicy(new RoundRobinPolicy())
                .Build();

            using (var session = cluster.Connect("keyspace1"))
            {
                var statement = session.Prepare("INSERT INTO table1(col0,col1,col2,col3,col4) VALUES (?,?,?,?,?)");
                var guids = Enumerable.Repeat(1, usersNumber).Select(x => Guid.NewGuid()).ToList();

                var stopwatch = Stopwatch.StartNew();

                await ConcurrentUtils.ConcurrentUtils.Times(usersNumber, usersNumber, index =>
                 session.ExecuteAsync(statement.Bind(guids[(int)index], "123456", DateTime.UtcNow, 1, "1234 some log message goes here. Hello world. 123334556567586978089-==00")));

                Console.WriteLine(usersNumber + " users insert at the same time:");
                Console.WriteLine("Total time: " + stopwatch.Elapsed);
                Console.WriteLine("Inserts/sec: " + (int)(usersNumber / stopwatch.Elapsed.TotalSeconds));
                Console.WriteLine("-----------------------------------------------------");
            }
        }

        private static async Task RunMongo(int usersNumber)
        {
            var settings = new MongoClientSettings();
            settings.Server = new MongoServerAddress("localhost");
            settings.WaitQueueSize = int.MaxValue;
            settings.WaitQueueTimeout = new TimeSpan(1, 0, 0);
            settings.MinConnectionPoolSize = 1;
            settings.MaxConnectionPoolSize = 512;

            var client = new MongoClient(settings);

            var database = client.GetDatabase("local");
            var collection = database.GetCollection<Collection1>("collection1");

            var guids = Enumerable.Repeat(1, usersNumber).Select(x => Guid.NewGuid()).ToList();

            var stopwatch = Stopwatch.StartNew();

            await ConcurrentUtils.ConcurrentUtils.Times(usersNumber, usersNumber, index =>
                 collection.InsertOneAsync(new Collection1
                 {
                     col0 = guids[(int)index],
                     col1 = "123456",
                     col2 = DateTime.UtcNow,
                     col3 = 1,
                     col4 = "1234 some log message goes here. Hello world. 123334556567586978089-==00"
                 }));

            Console.WriteLine(usersNumber + " users insert at the same time:");
            Console.WriteLine("Total time: " + stopwatch.Elapsed);
            Console.WriteLine("Inserts/sec: " + (int)(usersNumber / stopwatch.Elapsed.TotalSeconds));
            Console.WriteLine("-----------------------------------------------------");
        }
    }

    public class Collection1
    {
        [ScaffoldColumn(false)]
        [BsonId]
        public Guid col0 { get; set; }

        [ScaffoldColumn(false)]
        public string col1 { get; set; }

        [ScaffoldColumn(false)]
        public DateTime col2 { get; set; }

        [ScaffoldColumn(false)]
        public int col3 { get; set; }

        [ScaffoldColumn(false)]
        public string col4 { get; set; }
    }
}
