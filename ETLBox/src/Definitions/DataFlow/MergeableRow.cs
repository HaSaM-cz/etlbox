using System;
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
    /// <typeparam name="T">Derived type</typeparam>
    public abstract class MergeableRow<T> :
        MergeableRowBase<T>
        where T : MergeableRow<T>
    {
        protected MergeableRow() =>
            InitAttributeProperties();

        #region AttributeProperties

        protected AttributeProperties AttributeProperties => typeToAttributeProperties[GetType()];

        private void InitAttributeProperties()
        {
            Type curType = GetType();
            AttributeProperties curAttrProps;
            lock (typeToAttributeProperties)
            {
                if (!typeToAttributeProperties.TryGetValue(curType, out curAttrProps))
                {

                    curAttrProps = new AttributeProperties();
                    foreach (PropertyInfo propInfo in curType.GetProperties())
                    {
                        var idAttr = propInfo.GetCustomAttribute<IdColumn>();
                        if (idAttr != null)
                        {
                            var columnMap = propInfo.GetCustomAttribute<ColumnMap>();
                            string columnName = columnMap is null ?
                                propInfo.Name :
                                columnMap.ColumnName;
                            curAttrProps.IdColumnNames.Add((propInfo, columnName));
                        }
                        var compAttr = propInfo.GetCustomAttribute<CompareColumn>();
                        if (compAttr != null)
                            curAttrProps.CompareColumns.Add(propInfo);
                        var deleteAttr = propInfo.GetCustomAttribute<DeleteColumn>();
                        if (deleteAttr != null)
                            curAttrProps.DeleteColumnMatchValues.Add((propInfo, deleteAttr.DeleteOnMatchValue));
                    }
                    typeToAttributeProperties.Add(curType, curAttrProps);
                }
            }
        }

        private static readonly Dictionary<Type, AttributeProperties> typeToAttributeProperties = new Dictionary<Type, AttributeProperties>();

        #endregion

        public object GetValue(PropertyInfo property) => property?.GetValue(this);

        /// <summary>
        /// These are values of properties which have <see cref="IdColumn"/> attribute
        /// </summary>
        public override IEnumerable<object> IdValues => AttributeProperties.IdColumnNames.Select(i => GetValue(i.property));
        /// <summary>
        /// These are names (<see cref="ColumnMap.ColumnName"/>s) of properties which have <see cref="IdColumn"/> attribute (with optional <see cref="ColumnMap.ColumnName"/> attribute)
        /// </summary>
        public override IEnumerable<string> IdColumnNamesForDeletion => AttributeProperties.IdColumnNames.Select(i => i.columnName);
        /// <summary>
        /// These are values of properties which have <see cref="CompareColumn"/> attribute
        /// </summary>
        public override IEnumerable<object> ComparableValues => AttributeProperties.CompareColumns.Select(GetValue);

        /// <summary>
        /// Sets <see cref="ChangeAction"/> to <see cref="ChangeAction.Delete"/> if any property of this object has <see cref="DeleteColumn"/> attribute and all such properties values match <see cref="DeleteColumn.DeleteOnMatchValue"/>s
        /// </summary>
        public override void SetChangeAction()
        {
            var deleteColumnMatchValues = AttributeProperties.DeleteColumnMatchValues;
            if (
                deleteColumnMatchValues.Count > 0 &&
                deleteColumnMatchValues.All(i => EqualityComparer<object>.Default.Equals(GetValue(i.property), i.matchValue))
                )
            {
                ChangeAction = ETLBox.DataFlow.ChangeAction.Delete;
            }
        }
    }


    /// <summary>
    /// <see cref="MergeableRow{T}"/> for backwards compatibility
    /// </summary>
    public abstract class MergeableRow :
        MergeableRow<MergeableRow>
    { }
}
