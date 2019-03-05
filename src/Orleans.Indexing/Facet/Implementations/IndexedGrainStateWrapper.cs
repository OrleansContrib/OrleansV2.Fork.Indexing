using System;

namespace Orleans.Indexing.Facet
{
    /// <summary>
    /// A wrapper around a user-defined state, TGrainState, which indicates whether the grain has been persisted.
    /// </summary>
    /// <typeparam name="TGrainState">the type of user state</typeparam>
    [Serializable]
    public class IndexedGrainStateWrapper<TGrainState>
        where TGrainState: new()
    {
        /// <summary>
        /// Indicates whether the grain was read from storage (used on startup to set null values).
        /// </summary>
        public bool AreNullValuesInitialized;

        /// <summary>
        /// The actual user state.
        /// </summary>
        public TGrainState UserState = (TGrainState)Activator.CreateInstance(typeof(TGrainState));
    }
}
