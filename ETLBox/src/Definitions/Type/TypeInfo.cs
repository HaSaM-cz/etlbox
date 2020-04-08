using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class TypeInfo
    {
        internal PropertyInfo[] Properties { get; set; }
        protected Dictionary<string, int> PropertyIndex { get; set; } = new Dictionary<string, int>();
        internal int PropertyLength { get; set; }
        internal bool IsArray { get; set; } = true;
        internal bool IsDynamic { get; set; }
        internal int ArrayLength { get; set; }

        internal TypeInfo(Type type)
        {
            IsArray = type.IsArray;
            if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(type))
                IsDynamic = true;
            if (!IsArray && !IsDynamic)
            {
                Properties = type.GetProperties();
                PropertyLength = Properties.Length;
                int index = 0;
                foreach (var propInfo in Properties)
                {
                    PropertyIndex.Add(propInfo.Name, index);
                    RetrieveAdditionalTypeInfo(propInfo, index);
                    index++;
                }
            }
            else if (IsArray)
            {
                ArrayLength = type.GetArrayRank();
            }
        }

        internal static Type TryGetUnderlyingType(PropertyInfo propInfo)
        {
            return Nullable.GetUnderlyingType(propInfo.PropertyType) ?? propInfo.PropertyType;
        }

        protected virtual void RetrieveAdditionalTypeInfo(PropertyInfo propInfo, int currentIndex)
        {
            ;
        }


    }
}

