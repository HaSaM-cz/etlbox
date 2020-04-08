using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Inherit from this class if you want to use your data object with a <see cref="DbMerge{TInput}"/>.
    /// This implementation needs that you have flagged the id properties with the <see cref="IdColumn"/> attribute
    /// and the properties used to identify equal object flagged with the <see cref="CompareColumn"/> attribute.
    /// </summary>
    /// <see cref="CompareColumn"/>
    /// <see cref="IdColumn"/>
    public abstract class MergeableRow : IMergeableRow
    {
        private static readonly Dictionary<Type, AttributeProperties> typeToAttributeProperties = new Dictionary<Type, AttributeProperties>();

        public MergeableRow()
        {
            Type curType = this.GetType();
            AttributeProperties curAttrProps;
            lock (typeToAttributeProperties)
            {
                if (!typeToAttributeProperties.TryGetValue(curType, out curAttrProps))
                {

                    curAttrProps = new AttributeProperties();
                    foreach (PropertyInfo propInfo in curType.GetProperties())
                    {
                        var idAttr = propInfo.GetCustomAttribute(typeof(IdColumn)) as IdColumn;
                        if (idAttr != null)
                            curAttrProps.IdAttributeProps.Add(propInfo);
                        var compAttr = propInfo.GetCustomAttribute(typeof(CompareColumn)) as CompareColumn;
                        if (compAttr != null)
                            curAttrProps.CompareAttributeProps.Add(propInfo);
                        var deleteAttr = propInfo.GetCustomAttribute(typeof(DeleteColumn)) as DeleteColumn;
                        if (deleteAttr != null)
                            curAttrProps.DeleteAttributeProps.Add(Tuple.Create(propInfo, deleteAttr.DeleteOnMatchValue));
                    }
                    typeToAttributeProperties.Add(curType, curAttrProps);
                }
            }
        }

        /// <summary>
        /// <see cref="IMergeableRow.ChangeDate"/>
        /// </summary>
        public DateTime ChangeDate { get; set; }

        /// <summary>
        /// <see cref="IMergeableRow.ChangeAction"/>
        /// </summary>
        public ChangeAction? ChangeAction { get; set; }

        /// <summary>
        /// The UniqueId of the object. This is a concatenation evaluated from the properties
        /// which have the IdColumn attribute. if using an object as type, it is converted into a string
        /// using the ToString() method of the object.
        /// </summary>
        /// <see cref="IdColumn"/>
        public string UniqueId
        {
            get
            {
                AttributeProperties attrProps = typeToAttributeProperties[this.GetType()];
                string result = "";
                foreach (var propInfo in attrProps.IdAttributeProps)
                    result += propInfo?.GetValue(this).ToString();
                return result;
            }
        }

        public bool IsDeletion
        {
            get
            {
                AttributeProperties attrProps = typeToAttributeProperties[this.GetType()];
                bool result = true;
                foreach (var tup in attrProps.DeleteAttributeProps)
                    result &= (tup.Item1?.GetValue(this)).Equals(tup.Item2);
                return result;
            }
        }

        /// <summary>
        /// Overriding the Equals implementation. The Equals function is used identify records
        /// that don't need to be updated. Only properties marked with the CompareColumn attribute
        /// are considered for the comparison. If the property is of type object, the Equals() method of the object is used.
        /// </summary>
        /// <param name="other">Object to compare with. Should be of the same type.</param>
        /// <returns>True if all properties marked with CompareColumn attribute are equal.</returns>
        public override bool Equals(object other)
        {
            if (other == null) return false;
            AttributeProperties attrProps = typeToAttributeProperties[this.GetType()];
            bool result = true;
            foreach (var propInfo in attrProps.CompareAttributeProps)
                result &= (propInfo?.GetValue(this))?.Equals(propInfo?.GetValue(other)) ?? false;
            return result;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
