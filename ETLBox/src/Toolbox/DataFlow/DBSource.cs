using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    /// <example>
    /// <code>
    /// var source = new DbSource&lt;MyRow&gt;("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    public class DbSource<TOutput> :
        DataFlowSource<TOutput>,
        ITask,
        IDataFlowSource<TOutput>
        where TOutput : class
    {
        #region Init

        private DbSource(IConnectionManager connectionManager = null, Func<TOutput> createItem = null) :
            base(connectionManager)
        {
            typeInfo = new DbTypeInfo(typeof(TOutput));
            if (createItem is null && !typeInfo.IsArray)
                createItem = ItemFactory<TOutput>.CreateDefault;
            this.createItem = createItem;
        }

        public DbSource(TableDefinition tableDefinition, IConnectionManager connectionManager = null, Func<TOutput> createItem = null) :
            this(connectionManager, createItem)
            => TableDefinition = tableDefinition ?? throw new ArgumentNullException(nameof(tableDefinition));

        public DbSource(string sql, IConnectionManager connectionManager = null, Func<TOutput> createItem = null, IEnumerable<string> columnNames = null) :
            this(connectionManager, createItem)
        {
            if (sql is null)
                throw new ArgumentNullException(nameof(sql));
            if (string.IsNullOrWhiteSpace(sql))
                throw new ArgumentException("Value cannot be white space", nameof(sql));
            Sql = sql;
            if (columnNames != null)
                ColumnNames = columnNames.ToArray();
        }

        #endregion

        /* ITask Interface */
        public override string TaskName => $"Read data from {SourceDescription}";

        /* Public Properties */
        public bool HasTableDefinition => TableDefinition != null;
        public TableDefinition TableDefinition { get; }
        public bool HasSql => !string.IsNullOrWhiteSpace(Sql);
        public string Sql { get; }
        public IList<string> ColumnNames { get; set; }

        public string SqlForRead
        {
            get
            {
                if (HasSql)
                    return Sql;
                else
                {
                    TableDefinition.EnsureColumns(DbConnectionManager);
                    var TN = new ObjectNameDescriptor(TableDefinition.Name, ConnectionType);
                    return $@"SELECT {TableDefinition.Columns.AsString("", QB, QE)} FROM {TN.QuotatedFullName}";
                }

            }
        }

        public IList<string> ColumnNamesEvaluated
        {
            get
            {
                if (ColumnNames?.Count > 0)
                    return ColumnNames;
                else if (HasTableDefinition)
                    return TableDefinition?.Columns?.Select(col => col.Name).ToList();
                else
                    return ParseColumnNamesFromQuery();
            }
        }

        private readonly DbTypeInfo typeInfo;
        string SourceDescription
        {
            get
            {
                if (HasTableDefinition)
                    return $"table {TableDefinition.Name}";
                else
                    return "custom SQL";
            }
        }

        private List<string> ParseColumnNamesFromQuery()
        {
            var result = SqlParser.ParseColumnNames(QB != string.Empty ? SqlForRead.Replace(QB, "").Replace(QE, "") : SqlForRead);
            if (typeInfo.IsArray && result?.Count == 0) throw new ETLBoxException("Could not parse column names from Sql Query! Please pass a valid TableDefinition to the " +
                " property SourceTableDefinition with at least a name for each column that you want to use in the source."
                );
            return result;
        }

        public override void Execute()
        {
            NLogStart();
            try
            {
                ReadAll();
                Buffer.Complete();
            }
            catch (Exception e)
            {
                ((IDataflowBlock)Buffer).Fault(e);
                throw;
            }
            NLogFinish();
        }

        private void ReadAll()
        {
            SqlTask sqlTask = null;
            try
            {
                sqlTask = CreateSqlTask(SqlForRead);
                DefineActions(sqlTask, ColumnNamesEvaluated);
                sqlTask.ExecuteReader();
            }
            finally
            {
                if (sqlTask != null)
                    CleanupSqlTask(sqlTask);
            }
        }

        SqlTask CreateSqlTask(string sql)
        {
            var sqlTask = new SqlTask(this, sql)
            {
                DisableLogging = true,
            };
            sqlTask.Actions = new List<Action<object>>();
            return sqlTask;
        }

        TOutput _row;

        internal void DefineActions(SqlTask sqlTask, IList<string> columnNames)
        {
            if (columnNames is null)
                throw new ArgumentNullException(nameof(columnNames));
            _row = null;
            sqlTask.BeforeRowReadAction = () => _row = CreateItem();
            if (typeInfo.IsArray)
            {
                if (createItem is null)
                    createItem = () => ItemFactory<TOutput>.CreateArray(columnNames.Count);
                int index = 0;
                foreach (var colName in columnNames)
                    SetupArrayFillAction(sqlTask, index++);
            }
            else
            {
                if (columnNames.Count == 0) columnNames = typeInfo.PropertyNames.ToArray();
                foreach (var colName in columnNames)
                {
                    if (typeInfo.HasPropertyOrColumnMapping(colName))
                        SetupObjectFillAction(sqlTask, colName);
                    else if (typeInfo.IsDynamic)
                        SetupDynamicObjectFillAction(sqlTask, colName);
                    else
                        sqlTask.Actions.Add(col => { });
                }
            }
            sqlTask.AfterRowReadAction = () =>
            {
                if (_row != null)
                {
                    LogProgress();
                    Buffer.SendAsync(_row).Wait();
                }
            };
        }

        #region CreateItem

        public virtual TOutput CreateItem() => createItem?.Invoke();

        private Func<TOutput> createItem;

        #endregion

        private void SetupArrayFillAction(SqlTask sqlT, int index)
        {
            sqlT.Actions.Add(col =>
            {
                try
                {
                    if (_row != null)
                    {
                        var ar = _row as System.Array;
                        var con = Convert.ChangeType(col, typeof(TOutput).GetElementType());
                        ar.SetValue(con, index);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = null;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
                }
            });
        }

        private void SetupObjectFillAction(SqlTask sqlT, string colName)
        {
            sqlT.Actions.Add(colValue =>
            {
                try
                {
                    if (_row != null)
                    {
                        var propInfo = typeInfo.GetInfoByPropertyNameOrColumnMapping(colName);
                        var con = colValue != null ? Convert.ChangeType(colValue, typeInfo.UnderlyingPropType[propInfo]) : colValue;
                        propInfo.TrySetValue(_row, con);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = null;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
                }
            });
        }

        private void SetupDynamicObjectFillAction(SqlTask sqlT, string colName)
        {
            sqlT.Actions.Add(colValue =>
            {
                try
                {
                    if (_row != null)
                    {
                        typeInfo.CastDynamic(_row).Add(colName, colValue);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = null;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
                }
            });
        }

        void CleanupSqlTask(SqlTask sqlT)
        {
            sqlT.Actions = null;
        }


    }

    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets. The non generic version of the DbSource uses a dynamic object that contains the data.
    /// </summary>
    /// <see cref="DbSource{TOutput}"/>
    /// <example>
    /// <code>
    /// DbSource source = new DbSource("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    public class DbSource : DbSource<ExpandoObject>
    {
        public DbSource(TableDefinition tableDefinition, IConnectionManager connectionManager = null) :
            base(tableDefinition, connectionManager)
        { }

        public DbSource(string sql, IConnectionManager connectionManager = null) :
            base(sql, connectionManager)
        { }
    }
}
