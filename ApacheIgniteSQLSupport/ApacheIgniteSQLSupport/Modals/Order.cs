using Apache.Ignite.Core.Cache.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheIgniteSQLSupport
{
    public enum Side : short { Buy = 1, Sell = -1 }

    public sealed class Order
    {
        [QuerySqlField(IsIndexed = true)]
        public int Id { get; set; }

        [QuerySqlField(IsIndexed = true)]
        public string Symbol { get; set; } = "";

        [QuerySqlField]                 // price in major units (e.g., USD)
        public decimal Price { get; set; }

        [QuerySqlField]
        public int Qty { get; set; }

        [QuerySqlField]
        public Side Side { get; set; }  // Buy = +1, Sell = -1

        // Optional money fields (set to 0 when not applicable)
        [QuerySqlField] public decimal Commission { get; set; }
        [QuerySqlField] public decimal Fees { get; set; }
        [QuerySqlField] public decimal Tax { get; set; }

        // Routing / audit
        [QuerySqlField(IsIndexed = true)] public string Venue { get; set; } = "";
        [QuerySqlField(IsIndexed = true)] public string Account { get; set; } = "";

        // Timestamps (UTC)
        [QuerySqlField(IsIndexed = true)]
        public DateTime TimestampUtc { get; set; }

        // (Optional) persist precomputed amount for faster SQL analytics
        [QuerySqlField] public decimal Amount => Price * Qty;
    }
}
