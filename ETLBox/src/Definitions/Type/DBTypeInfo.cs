using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class DbTypeInfo : TypeInfo
    {
        public DbTypeInfo(Type type) :
            base(type)
        { }

        public IReadOnlyDictionary<string, string> ColumnNameToPropertyName => columnNameToPropertyName;
        public IReadOnlyDictionary<PropertyInfo, Type> UnderlyingPropType => underlyingPropType;

        protected override void RetrieveAdditionalTypeInfo(PropertyInfo property, int index)
        {
            AddColumnMappingAttribute(property);
            AddUnderlyingType(property);

        }

        private void AddColumnMappingAttribute(PropertyInfo propInfo)
        {
            var attr = propInfo.GetCustomAttribute<ColumnMap>();
            if (attr != null)
                columnNameToPropertyName.Add(attr.ColumnName, propInfo.Name);
        }

        private void AddUnderlyingType(PropertyInfo propInfo)
        {
            Type t = TypeInfo.TryGetUnderlyingType(propInfo);
            underlyingPropType.Add(propInfo, t);
        }

        public bool HasPropertyOrColumnMapping(string name)
        {
            if (ColumnNameToPropertyName.ContainsKey(name))
                return true;
            else
                return PropertyNames.Contains(name);
        }
        public PropertyInfo GetInfoByPropertyNameOrColumnMapping(string propNameOrColMapName)
        {
            PropertyInfo result = null;
            if (ColumnNameToPropertyName.ContainsKey(propNameOrColMapName))
                result = Properties[PropertyNameToIndex[ColumnNameToPropertyName[propNameOrColMapName]]];
            else
                result = Properties[PropertyNameToIndex[propNameOrColMapName]];
            return result;
        }

        public int GetIndexByPropertyNameOrColumnMapping(string propNameOrColMapName)
        {
            if (ColumnNameToPropertyName.ContainsKey(propNameOrColMapName))
                return PropertyNameToIndex[ColumnNameToPropertyName[propNameOrColMapName]];
            else
                return PropertyNameToIndex[propNameOrColMapName];
        }

        private readonly Dictionary<string, string> columnNameToPropertyName = new Dictionary<string, string>();
        private readonly Dictionary<PropertyInfo, Type> underlyingPropType = new Dictionary<PropertyInfo, Type>();
    }
}

