using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Dynamic <see cref="IMergeableRow"/> with <see cref="ExpandoObject"/>
    /// </summary>
    /// <remarks><see cref="IDictionary{TKey, TValue}"/> implementation is via <see cref="Values"/></remarks>
    public class DynamicMergeableRow :
        MergeableRowBase<DynamicMergeableRow>,
        IDictionary<string, object>
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="idColumnNames">Property names for <see cref="IdValues"/></param>
        /// <param name="comparableColumnNames">Property names for <see cref="ComparableValues"/></param>
        /// <param name="isDeletedColumnName">Optional name of property whose value is to be checked for equality with <paramref name="isDeletedValue"/> (using <see cref="Equality.ValueEquals(object, object)"/>) to determine whether this row should be deleted</param>
        /// <param name="isDeletedValue">Value used with <paramref name="isDeletedColumnName"/></param>
        /// <exception cref="ArgumentNullException"><paramref name="idColumnNames"/> is null</exception>
        /// <exception cref="ArgumentException"><paramref name="idColumnNames"/> is empty</exception>
        public DynamicMergeableRow(
            IEnumerable<string> idColumnNames,
            IEnumerable<string> comparableColumnNames = null,
            string isDeletedColumnName = null,
            object isDeletedValue = null
            )
        {
            this.idColumnNames = idColumnNames ?? throw new ArgumentNullException(nameof(idColumnNames));
            if (!idColumnNames.Any())
                throw new ArgumentException("Value cannot be empty", nameof(idColumnNames));
            this.comparableColumnNames = comparableColumnNames ?? Enumerable.Empty<string>();
            this.isDeletedColumnName = isDeletedColumnName;
            this.isDeletedValue = isDeletedValue;
        }
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="tableDefinition">Table definition to get <see cref="MergeableRowBase{T}.Id"/> property names for <see cref="IdValues"/> with <see cref="GetIdPropertyNames"/></param>
        /// <param name="comparableColumnNames">Property names for <see cref="ComparableValues"/></param>
        /// <param name="isDeletedColumnName">Optional name of property whose value is to be checked for equality with <paramref name="isDeletedValue"/> (using <see cref="Equality.ValueEquals(object, object)"/>) to determine whether this row should be deleted</param>
        /// <param name="isDeletedValue">Value used with <paramref name="isDeletedColumnName"/></param>
        /// <exception cref="ArgumentNullException"><paramref name="tableDefinition"/> is null</exception>
        public DynamicMergeableRow(
            TableDefinition tableDefinition,
            IEnumerable<string> comparableColumnNames = null,
            string isDeletedColumnName = null,
            object isDeletedValue = null
            ) :
            this(GetIdPropertyNames(tableDefinition), comparableColumnNames, isDeletedColumnName, isDeletedValue)
        { }

        /// <summary>
        /// Gets <see cref="MergeableRowBase{T}.Id"/> property names from primary key <see cref="TableDefinition.Columns"/>
        /// </summary>
        /// <param name="tableDefinition">Table definition</param>
        /// <exception cref="ArgumentNullException"><paramref name="tableDefinition"/> is null</exception>
        public static IEnumerable<string> GetIdPropertyNames(TableDefinition tableDefinition)
        {
            if (tableDefinition is null)
                throw new ArgumentNullException(nameof(tableDefinition));
            return tableDefinition.Columns.PrimaryKey().Names();
        }

        /// <summary>
        /// Dynamic values
        /// </summary>
        /// <value><see cref="ExpandoObject"/></value>
        public dynamic Values { get; } = new ExpandoObject();

        #region IDictionary

        private IDictionary<string, object> ValuesDictionary => (IDictionary<string, object>)Values;

        public void Add(string key, object value) => ValuesDictionary.Add(key, value);
        public bool ContainsKey(string key) => ValuesDictionary.ContainsKey(key);
        public bool Remove(string key) => ValuesDictionary.Remove(key);
        public bool TryGetValue(string key, out object value) => ValuesDictionary.TryGetValue(key, out value);

        public object this[string key]
        {
            get => ValuesDictionary[key];
            set => ValuesDictionary[key] = value;
        }

        public ICollection<string> Keys => ValuesDictionary.Keys;

        ICollection<object> IDictionary<string, object>.Values => ValuesDictionary.Values;

        public void Add(KeyValuePair<string, object> item) => ValuesDictionary.Add(item);
        public void Clear() => ValuesDictionary.Clear();
        public bool Contains(KeyValuePair<string, object> item) => ValuesDictionary.Contains(item);
        public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex) => ValuesDictionary.CopyTo(array, arrayIndex);
        public bool Remove(KeyValuePair<string, object> item) => ValuesDictionary.Remove(item);

        public int Count => ValuesDictionary.Count;

        public bool IsReadOnly => ValuesDictionary.IsReadOnly;

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => ValuesDictionary.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => ValuesDictionary.GetEnumerator();

        #endregion

        public override IEnumerable<object> IdValues => GetValues(idColumnNames);
        public override IEnumerable<object> ComparableValues => GetValues(comparableColumnNames);

        public IEnumerable<object> GetValues(IEnumerable<string> names) => names?.Select(i => this[i]);

        public override void SetChangeAction()
        {
            if (
                !string.IsNullOrWhiteSpace(isDeletedColumnName) &&
                ValueEquals(this[isDeletedColumnName], isDeletedValue)
                )
            {
                ChangeAction = ETLBox.DataFlow.ChangeAction.Delete;
            }
        }

        protected virtual bool ValueEquals(object value1, object value2) => value1.ValueEquals(value2);

        private readonly IEnumerable<string> idColumnNames;
        private readonly IEnumerable<string> comparableColumnNames;
        private readonly string isDeletedColumnName;
        private readonly object isDeletedValue;
    }
}
