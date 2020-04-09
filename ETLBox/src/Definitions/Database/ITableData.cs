using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox
{
    public interface ITableData : IDataReader
    {
        IColumnMappingCollection ColumnMapping { get; }
        IReadOnlyList<IReadOnlyList<object>> Rows { get; }
    }
}
