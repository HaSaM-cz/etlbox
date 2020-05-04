using System;

namespace ALE.ETLBox
{
    /// <summary>
    /// Data flow progress
    /// </summary>
    public interface IDataFlowProgress
    {
        /// <summary>
        /// Number of processed items
        /// </summary>
        ulong ProgressCount { get; }
        /// <summary>
        /// Raised when <see cref="ProgressCount"/> changes
        /// </summary>
        event EventHandler<ulong> ProgressCountChanged;
    }
}