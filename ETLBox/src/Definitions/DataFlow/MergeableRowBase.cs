using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// <see cref="IMergeableRow"/> base
    /// </summary>
    /// <typeparam name="T">Derived type</typeparam>
    public abstract class MergeableRowBase<T> :
        IMergeableRow
        where T : MergeableRowBase<T>
    {
        #region Id

        /// <summary>
        /// <see cref="IMergeableRow.Id"/>
        /// </summary>
        /// <value>Concatenated <see cref="IdValues"/> (converted into a string using <see cref="object.ToString"/>)</value>
        public string Id
        {
            get
            {
                var values = IdValues.Select(ToSqlString);
                string value = string.Concat(values);
                if (string.IsNullOrEmpty(value))
                {
                    string valuesText = string.Join(", ", values);
                    throw new ArgumentOutOfRangeException(
                        nameof(Id), value,
                        $"Value cannot be null or white space. Please check {nameof(IdValues)} implementation and data ({valuesText})."
                        );
                }
                return value;
            }
        }

        /// <summary>
        /// Converts <paramref name="value"/> to SQL text representation
        /// </summary>
        /// <param name="value">Property value</param>
        protected virtual string ToSqlString(object value) => value switch
        {
            DateTime t => t.ToString("o"),
            DateTimeOffset t => t.ToString("o"),
            TimeSpan t => t.ToString("c"),
            null => "null",
            _ => value.ToString()
        };

        /// <summary>
        /// Values (primary key values) <see cref="Id"/> is composed of
        /// </summary>
        public abstract IEnumerable<object> IdValues { get; }

        #endregion

        #region Change

        /// <summary>
        /// <see cref="IMergeableRow.ChangeAction"/>
        /// </summary>
        public ChangeAction? ChangeAction
        {
            get => changeAction;
            set
            {
                changeAction = value;
                SetChangeTime();
            }
        }
        /// <summary>
        /// <see cref="IMergeableRow.ChangeTime"/>
        /// </summary>
        public DateTime? ChangeTime
        {
            get => changeDate;
            set
            {
                if (ChangeAction is null)
                {
                    if (value != null)
                        throw new ArgumentOutOfRangeException(nameof(ChangeTime), value, $"Value cannot be specified if {nameof(ChangeAction)} is null");
                }
                else
                {
                    if (value is null)
                        throw new ArgumentOutOfRangeException(nameof(ChangeTime), value, $"Value cannot be null if {nameof(ChangeAction)} is {ChangeAction}");
                }
                changeDate = value;
            }
        }

        /// <summary>
        /// <see cref="IMergeableRow.SetChangeAction"/>
        /// </summary>
        /// <remarks>This implementation does nothing</remarks>
        public virtual void SetChangeAction() { }
        /// <summary>
        /// <see cref="IMergeableRow.SetChangeTime"/>
        /// </summary>
        public void SetChangeTime() => ChangeTime = ChangeAction.HasValue ?
            DateTime.Now :
            (DateTime?)null;

        private ChangeAction? changeAction;
        private DateTime? changeDate;

        #endregion

        #region Equals

        /// <summary>
        /// Values to be compared for equality without <see cref="IdValues"/> included
        /// </summary>
        /// <value>
        /// non-null.
        /// This implementation returns empty enumerable
        /// </value>
        public virtual IEnumerable<object> ComparableValues => Enumerable.Empty<object>();

        public sealed override bool Equals(object obj) =>
            obj is T other &&
            Equals(other);

        bool IEquatable<IMergeableRow>.Equals(IMergeableRow other) =>
            other is T o &&
            Equals(o);

        public bool Equals(T other) =>
            other != null &&
            EqualsId(other) &&
            EqualsWithoutId(other);

        public bool EqualsId(T other) =>
            other != null &&
            DoEqualsId(other);

        protected virtual bool DoEqualsId(T other) => IdValues.SequenceEqual(other.IdValues); // prevents IdValues conversion to string Id

        bool IMergeableRow.EqualsWithoutId(IMergeableRow other) =>
            other is T o &&
            EqualsWithoutId(o);

        public bool EqualsWithoutId(T other) =>
            other != null &&
            DoEqualsWithoutId(other);

        protected virtual bool DoEqualsWithoutId(T other) => ComparableValues.SequenceEqual(other.ComparableValues);

        public sealed override int GetHashCode() =>
            HashCodeId ^
            HashCodeWithoutId;

        public static int GetHashCode(IEnumerable<object> values) => values?.Sum(i => i.GetHashCode()) ?? 0;

        public virtual int HashCodeId => GetHashCode(IdValues);
        public virtual int HashCodeWithoutId => GetHashCode(ComparableValues);

        #endregion
    }
}
