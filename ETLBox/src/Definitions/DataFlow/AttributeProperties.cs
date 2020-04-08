using System.Collections.Generic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Properties used by <see cref="MergeableRow"/>
    /// </summary>
    public class AttributeProperties
    {
        public List<(PropertyInfo property, string columnName)> IdColumnNames { get; } = new List<(PropertyInfo property, string columnName)>();
        public List<PropertyInfo> CompareColumns { get; } = new List<PropertyInfo>();
        public List<(PropertyInfo property, object matchValue)> DeleteColumnMatchValues { get; } = new List<(PropertyInfo, object)>();
    }
}
