using System;
using System.Collections.Generic;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Dynamic type helper
    /// </summary>
    public static class Dynamics
    {
        public static bool IsDynamic(this object item) => item is IDictionary<string, object>;
        public static bool IsDynamic(this Type type) => typeof(IDictionary<string, object>).IsAssignableFrom(type);

        public static IDictionary<string, object> CastDynamic(this object item) => (IDictionary<string, object>)item;
    }
}
