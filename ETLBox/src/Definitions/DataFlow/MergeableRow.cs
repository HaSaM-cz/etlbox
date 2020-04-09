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
        protected MergeableRowProperties Properties => MergeableRowProperties.Of(GetType());

        public object GetValue(PropertyInfo property) => property?.GetValue(this);

        /// <summary>
        /// These are values of properties which have <see cref="IdColumn"/> attribute
        /// </summary>
        public override IEnumerable<object> IdValues => Properties.IdColumnNames.Select(i => GetValue(i.property));
        /// <summary>
        /// These are values of properties which have <see cref="CompareColumn"/> attribute
        /// </summary>
        public override IEnumerable<object> ComparableValues => Properties.CompareColumns.Select(GetValue);

        /// <summary>
        /// Sets <see cref="ChangeAction"/> to <see cref="ChangeAction.Delete"/> if any property of this object has <see cref="DeleteColumn"/> attribute and all such properties values match <see cref="DeleteColumn.DeleteOnMatchValue"/>s
        /// </summary>
        public override void SetChangeAction()
        {
            var deleteColumnMatchValues = Properties.DeleteColumnMatchValues;
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
