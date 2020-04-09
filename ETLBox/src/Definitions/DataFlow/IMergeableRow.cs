using System;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Represents table row in destination database for <see cref="DbMerge{TInput}"/>
    /// </summary>
    public interface IMergeableRow :
        IEquatable<IMergeableRow>
    {
        /// <summary>
        /// Unique row identifier (textual concatenation of primary key destination table column values)
        /// </summary>
        /// <value>not white space</value>
        string Id { get; }
        /// <summary>
        /// The result of a merge operation
        /// </summary>
        /// <remarks><see cref="SetChangeTime"/> is called when this value is set</remarks>
        /// <value>null means not determined yet</value>
        ChangeAction? ChangeAction { get; set; }
        /// <summary>
        /// Time when the object was considered for merging
        /// </summary>
        /// <remarks>
        /// When <see cref="ChangeAction"/> is set, this value is set to <see cref="DateTime.Now"/>, but it can be changed later to another value if required
        /// </remarks>
        /// <value>null means not determined yet</value>
        /// <exception cref="ArgumentOutOfRangeException">
        /// <see cref="ChangeAction"/> is specified/null and this value is going to be set to null/<see cref="DateTime"/>
        /// </exception>
        DateTime? ChangeTime { get; set; }

        /// <summary>
        /// Updates <see cref="ChangeAction"/> if possible using internal logic
        /// </summary>
        void SetChangeAction();
        /// <summary>
        /// Updates <see cref="ChangeTime"/> to <see cref="DateTime.Now"/>/null if <see cref="ChangeAction"/> is <see cref="ChangeAction"/>/null
        /// </summary>
        void SetChangeTime();
        /// <summary>
        /// Compares this object with <paramref name="other"/> for equality while ignoring <see cref="Id"/>s
        /// </summary>
        /// <param name="other">Other object</param>
        bool EqualsWithoutId(IMergeableRow other);
    }
}
