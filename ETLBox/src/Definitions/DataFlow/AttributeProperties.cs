using System;
using System.Collections.Generic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    public class AttributeProperties
    {
        public List<PropertyInfo> IdAttributeProps { get; } = new List<PropertyInfo>();
        public List<PropertyInfo> CompareAttributeProps { get; } = new List<PropertyInfo>();
        public List<Tuple<PropertyInfo, object>> DeleteAttributeProps { get; } = new List<Tuple<PropertyInfo, object>>();
    }
}
