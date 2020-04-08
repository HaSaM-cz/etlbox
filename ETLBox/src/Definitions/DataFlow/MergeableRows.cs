using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// <see cref="IMergeableRow"/> helper
    /// </summary>
    public static class MergeableRows
    {
        public static IEnumerable<T> WithChangeAction<T>(this IEnumerable<T> rows, ChangeAction? changeAction)
            where T : IMergeableRow
            => rows?.Where(i => i.ChangeAction == changeAction);
    }
}
