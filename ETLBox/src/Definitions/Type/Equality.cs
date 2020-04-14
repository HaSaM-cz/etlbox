using System;
using System.Collections.Generic;
using System.Globalization;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Equality helper
    /// </summary>
    public static class Equality
    {
        /// <summary>
        /// Whether <paramref name="value1"/> equals <paramref name="value2"/>
        /// </summary>
        /// <param name="value1">First value</param>
        /// <param name="value2">Second value</param>
        /// <returns>
        /// If value types differ, but both are not null, <paramref name="value2"/> is converted to type of <paramref name="value1"/> (with <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/> and <see cref="CultureInfo.InvariantCulture"/>) first.
        /// Result of <see cref="EqualityComparer{T}.Equals(T, T)"/> is returned.
        /// </returns>
        public static bool ValueEquals(this object value1, object value2)
        {
            var type1 = value1?.GetType();
            var type2 = value2?.GetType();
            // different types, try conversion (eg. from string)
            if (
                type1 != type2 &&
                type1 != null &&
                type2 != null
                )
            {
                value2 = Convert.ChangeType(value2, type1, CultureInfo.InvariantCulture);
            }
            return EqualityComparer<object>.Default.Equals(value1, value2);
        }
    }
}
