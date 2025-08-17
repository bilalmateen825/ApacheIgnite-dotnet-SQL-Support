using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.DataStructures;
using Apache.Ignite.Core.Transactions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheIgniteSQLSupport
{
    internal class WritePathAggregatorService : BackgroundService
    {
        private readonly ILogger<WritePathAggregatorService> _log;
        private IIgnite _ignite = default!;
        private ICache<int, Order> _orders = default!;
        private AtomicLongEx _totalCents = default!;

        public WritePathAggregatorService(ILogger<WritePathAggregatorService> log) => _log = log;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 1) Start a thick client node (client mode = true) to access data structures (AtomicLong).
            _ignite = Ignition.Start(new Apache.Ignite.Core.IgniteConfiguration
            {
                ClientMode = true,               // IMPORTANT: thick client, not thin client
                                                 // Optional: set DiscoverySpi, endpoints etc via XML or programmatic config if needed.
            });

            // 2) Create cache schema once (idempotent).
            _orders = _ignite.GetOrCreateCache<int, Order>(
                new CacheConfiguration("orders")
                {
                    AtomicityMode = CacheAtomicityMode.Transactional,
                    Backups = 1,
                    QueryEntities = new[] { new QueryEntity(typeof(int), typeof(Order)) { TableName = "Orders" } }
                });

            // 3) Create/read the running total in **cents**.
            _totalCents = new AtomicLongEx(_ignite.GetAtomicLong("orders_total_cents", 0, create: true));

            // 4) Bootstrap the running total once (or when you deploy).
            await BootstrapTotalFromSqlAsync();

            _log.LogInformation("Write-path aggregator started. Ready to accept writes.");

            // Simulate: if you want to demo inserts, uncomment:
            // await DemoAsync();

            // Keep service alive.
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        // --- Public API you expose to your producers (Web API, gRPC, whatever) ---

        public decimal AddOrder(Order o)
        {
            using var tx = _ignite.GetTransactions().TxStart(
                TransactionConcurrency.Pessimistic, TransactionIsolation.ReadCommitted);

            _orders.Put(o.Id, o);
            var delta = ToCents(o.Price * o.Qty);
            var newTotal = _totalCents.AddAndGet(delta);
            tx.Commit();

            PublishToClients(newTotal);
            return FromCents(newTotal);
        }

        public decimal UpsertOrder(Order updated)
        {
            using var tx = _ignite.GetTransactions().TxStart(
                TransactionConcurrency.Pessimistic, TransactionIsolation.ReadCommitted);

            var existed = _orders.Get(updated.Id);
            var oldAmt = existed is null ? 0m : existed.Price * existed.Qty;
            var newAmt = updated.Price * updated.Qty;

            _orders.Put(updated.Id, updated);

            var delta = ToCents(newAmt - oldAmt);
            var newTotal = _totalCents.AddAndGet(delta);
            tx.Commit();

            PublishToClients(newTotal);
            return FromCents(newTotal);
        }

        public decimal DeleteOrder(int id)
        {
            using var tx = _ignite.GetTransactions().TxStart(
                TransactionConcurrency.Pessimistic, TransactionIsolation.ReadCommitted);

            var old = _orders.Get(id);
            if (old is not null)
            {
                _orders.Remove(id);
                var delta = -ToCents(old.Price * old.Qty);
                var newTotal = _totalCents.AddAndGet(delta);
                tx.Commit();

                PublishToClients(newTotal);
                return FromCents(newTotal);
            }

            tx.Commit();
            var current = _totalCents.Read();
            return FromCents(current);
        }

        // --- Helpers ---

        private async Task BootstrapTotalFromSqlAsync()
        {
            // SELECT COALESCE(SUM(Price*Qty), 0) FROM Orders
            var q = new Apache.Ignite.Core.Cache.Query.SqlFieldsQuery(
                "select coalesce(sum(price * qty), 0) from Orders");

            var res = _orders.Query(q).GetAll();
            var dec = (decimal)res[0][0];

            _totalCents.Exchange(ToCents(dec));

            _log.LogInformation("Bootstrapped total to {Total}", dec);
            await Task.CompletedTask;
        }

        private static long ToCents(decimal money) =>
            (long)Math.Round(money * 100m, MidpointRounding.AwayFromZero);

        private static decimal FromCents(long cents) => cents / 100m;

        private void PublishToClients(long newTotalCents)
        {
            // Hook your transport here: SignalR/gRPC/Ignite Messaging.
            // Example (pseudo):
            // _broadcaster.PushTotal(FromCents(newTotalCents));
            _log.LogDebug("New total published: {Total}", FromCents(newTotalCents));
        }

        // Optional demo:
        private async Task DemoAsync()
        {
            AddOrder(new Order { Id = 1, Price = 10m, Qty = 20 }); // 200
            AddOrder(new Order { Id = 2, Price = 5m, Qty = 5 }); // +25 => 225
            await Task.Delay(10);
        }
    }

    /// Tiny wrapper so callers can’t accidentally mix units.
    internal sealed class AtomicLongEx
    {
        private readonly Apache.Ignite.Core.DataStructures.IAtomicLong _inner;
        public AtomicLongEx(Apache.Ignite.Core.DataStructures.IAtomicLong inner) => _inner = inner;
        public long AddAndGet(long delta) => _inner.Add(delta);
        public long Read() => _inner.Read();
        public long Exchange(long v) => _inner.Exchange(v);
    }
}
