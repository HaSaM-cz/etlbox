using System;

namespace ALE.ETLBox.DataFlow
{
    public interface IMergeableRow
    {
        /// <summary>
        /// Date and time when the object was considered for merging
        /// </summary>
        DateTime ChangeDate { get; set; }
        /// <summary>
        /// The result of a merge operation
        /// </summary>
        /// <value>null means not determined yet</value>
        ChangeAction? ChangeAction { get; set; }
        string UniqueId { get; }
        bool IsDeletion { get; }
    }
}
