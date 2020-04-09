using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class TypeInfo
    {
        public TypeInfo(Type type)
        {
            if (type is null)
                throw new ArgumentNullException(nameof(type));
            IsArray = type.IsArray;
            IsDynamic = IsDynamicType(type);
            if (!IsArray && !IsDynamic)
            {
                Properties = type.GetProperties();
                FillPropertyNameToIndex();
                RetrieveAdditionalTypeInfos();
            }
        }

        public static Type TryGetUnderlyingType(PropertyInfo propInfo)
        {
            return Nullable.GetUnderlyingType(propInfo.PropertyType) ?? propInfo.PropertyType;
        }

        #region AdditionalTypeInfo

        private void RetrieveAdditionalTypeInfos()
        {
            int index = 0;
            foreach (var propInfo in Properties)
                RetrieveAdditionalTypeInfo(propInfo, index++);
        }

        protected virtual void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        { }

        #endregion

        #region Properties

        public IReadOnlyList<PropertyInfo> Properties { get; }

        public IEnumerable<string> PropertyNames => IsDynamic ?
            DynamicPropertyNames is null ?
                Enumerable.Empty<string>() :
                DynamicPropertyNames :
            Properties.Select(i => i.Name);

        protected IReadOnlyDictionary<string, int> PropertyNameToIndex => propertyNameToIndex;

        private void FillPropertyNameToIndex()
        {
            int index = 0;
            foreach (var name in PropertyNames)
                propertyNameToIndex.Add(name, index++);
        }

        private readonly Dictionary<string, int> propertyNameToIndex = new Dictionary<string, int>();

        #endregion

        #region Array

        public bool IsArray { get; }

        #endregion

        #region Dynamic

        public bool IsDynamic { get; }

        public static bool IsDynamicType(Type type) => typeof(IDictionary<string, object>).IsAssignableFrom(type);

        public IReadOnlyList<string> DynamicPropertyNames { get; private set; }

        public IDictionary<string, object> CastDynamic(object item) => (IDictionary<string, object>)item;

        public void FillDynamicPropertyNames(object item)
        {
            if (IsDynamic && DynamicPropertyNames != null)
                return;
            DynamicPropertyNames = CastDynamic(item).Keys.ToArray();
            FillPropertyNameToIndex();
        }

        #endregion
    }
}

