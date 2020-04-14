using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// <see cref="IMergeableRow"/> helper
    /// </summary>
    public static class MergeableRows
    {
        public static IEnumerable<T> WithChangeAction<T>(this IEnumerable<T> rows, params ChangeAction?[] changeActions)
            where T : IMergeableRow
            => rows?.Where(i => changeActions.Contains(i.ChangeAction));

        public static IEnumerable<T> WithoutChangeAction<T>(this IEnumerable<T> rows, params ChangeAction?[] changeActions)
            where T : IMergeableRow
            => rows?.Where(i => !changeActions.Contains(i.ChangeAction));
    }
}
