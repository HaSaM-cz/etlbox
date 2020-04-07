using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox
{
    /// <summary>
    /// <see cref="TableColumn"/> helper
    /// </summary>
    public static class TableColumns
    {
        /// <summary>
        /// <see cref="TableColumn.Name"/>s
        /// </summary>
        /// <param name="columns">Columns</param>
        /// <returns>Names if <paramref name="columns"/> is not null, otherwise null</returns>
        public static IEnumerable<string> Names(this IEnumerable<TableColumn> columns) => columns?.Select(i => i.Name);

        /// <summary>
        /// Finds first column with <paramref name="name"/>
        /// </summary>
        /// <param name="columns">Columns</param>
        /// <param name="name">Required <see cref="TableColumn.Name"/></param>
        /// <param name="required">Whether a column must be found</param>
        /// <returns>Column if found, otherwise null</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="required"/> is true and no column was found</exception>
        public static TableColumn WithName(this IEnumerable<TableColumn> columns, string name, bool required = false)
        {
            var column = columns?.FirstOrDefault(i => i.Name == name);
            if (required && column is null)
                throw new ArgumentOutOfRangeException(nameof(name), name, $"Column {name} was not found");
            return column;
        }

        /// <summary>
        /// Finds columns with <paramref name="names"/>
        /// </summary>
        /// <param name="columns">Columns</param>
        /// <param name="names">Required <see cref="TableColumn.Name"/>s</param>
        /// <param name="required">Whether a column must be found</param>
        /// <returns>Columns with <paramref name="names"/> in same order as names if found, otherwise empty if <paramref name="names"/> is not null, otherwise null</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="required"/> is true and a column was not found</exception>
        public static IEnumerable<TableColumn> WithNames(this IEnumerable<TableColumn> columns, bool required, params string[] names) => names?.
            Select(i => columns.WithName(i, required)).
            Where(i => i != null);

        /// <summary>
        /// Gets primary key columns (with <see cref="TableColumn.IsPrimaryKey"/>)
        /// </summary>
        /// <param name="columns">Columns</param>
        /// <returns>Columns if found, otherwise empty if <paramref name="columns"/> is not null, otherwise null</returns>
        public static IEnumerable<TableColumn> PrimaryKey(this IEnumerable<TableColumn> columns) => columns?.Where(i => i.IsPrimaryKey);

        /// <summary>
        /// Gets identity columns (with <see cref="TableColumn.IsIdentity"/>)
        /// </summary>
        /// <param name="columns">Columns</param>
        /// <returns>Columns if found, otherwise empty if <paramref name="columns"/> is not null, otherwise null</returns>
        public static IEnumerable<TableColumn> Identity(this IEnumerable<TableColumn> columns) => columns?.Where(i => i.IsIdentity);
    }
}
