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
        /// <returns>Column if found, otherwise null</returns>
        public static TableColumn Column(this IEnumerable<TableColumn> columns, string name) => columns?.FirstOrDefault(i => i.Name == name);
    }
}
