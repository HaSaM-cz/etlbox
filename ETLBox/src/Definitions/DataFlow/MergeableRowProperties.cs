using System;
using System.Collections.Generic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Properties used by <see cref="MergeableRow{T}"/>
    /// </summary>
    public class MergeableRowProperties
    {
        public List<(PropertyInfo property, string columnName)> IdColumnNames { get; } = new List<(PropertyInfo property, string columnName)>();
        public List<PropertyInfo> CompareColumns { get; } = new List<PropertyInfo>();
        public List<(PropertyInfo property, object matchValue)> DeleteColumnMatchValues { get; } = new List<(PropertyInfo, object)>();

        public static MergeableRowProperties Of(Type type)
        {
            if (type is null)
                throw new ArgumentNullException(nameof(type));
            MergeableRowProperties value;
            lock (cache)
            {
                if (!cache.TryGetValue(type, out value))
                {

                    value = new MergeableRowProperties();
                    foreach (PropertyInfo propInfo in type.GetProperties())
                    {
                        var idAttr = propInfo.GetCustomAttribute<IdColumn>();
                        if (idAttr != null)
                        {
                            var columnMap = propInfo.GetCustomAttribute<ColumnMap>();
                            string columnName = columnMap is null ?
                                propInfo.Name :
                                columnMap.ColumnName;
                            value.IdColumnNames.Add((propInfo, columnName));
                        }
                        var compAttr = propInfo.GetCustomAttribute<CompareColumn>();
                        if (compAttr != null)
                            value.CompareColumns.Add(propInfo);
                        var deleteAttr = propInfo.GetCustomAttribute<DeleteColumn>();
                        if (deleteAttr != null)
                            value.DeleteColumnMatchValues.Add((propInfo, deleteAttr.DeleteOnMatchValue));
                    }
                    cache.Add(type, value);
                }
                return value;
            }
        }

        private static readonly Dictionary<Type, MergeableRowProperties> cache = new Dictionary<Type, MergeableRowProperties>();
    }
}
