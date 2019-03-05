using System;

namespace Orleans.Indexing.Facet
{
    /// <summary>
    /// Marker interface for indexed state management.
    /// </summary>
    public class IndexedStateAttribute : Attribute
    {
        public string StorageName { get; private protected set; }
    }
}
