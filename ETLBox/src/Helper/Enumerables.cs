using System;
using System.Collections.Generic;

namespace ALE.ETLBox.Helper
{
    /// <summary>
    /// <see cref="IEnumerable{T}"/> helper
    /// </summary>
    public static class Enumerables
    {
        /// <summary>
        /// Partitions <paramref name="items"/> into batches of specified <paramref name="size"/>
        /// </summary>
        /// <remarks>Source: https://stackoverflow.com/a/44505349/7821542</remarks>
        /// <typeparam name="T">Item type</typeparam>
        /// <param name="items">Items</param>
        /// <param name="size">Batch size</param>
        /// <returns>Batches</returns>
        /// <exception cref="ArgumentNullException"><paramref name="items"/> is null</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="size"/> is not positive</exception>
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> items, int size)
        {
            if (items is null)
                throw new ArgumentNullException(nameof(items));
            if (size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size), size, "Value must be positive");
            using var enumerator = items.GetEnumerator();
            while (enumerator.MoveNext())
            {
                int i = 0;
                // Batch is a local function closing over `i` and `enumerator` that executes the inner batch enumeration
                IEnumerable<T> Batch()
                {
                    do yield return enumerator.Current;
                    while (++i < size && enumerator.MoveNext());
                }

                yield return Batch();
                while (++i < size && enumerator.MoveNext()) ; // discard skipped items
            }
        }
    }
}