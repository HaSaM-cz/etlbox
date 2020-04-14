using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Inserts, updates and (optionally) deletes data in db target.
    /// </summary>
    /// <typeparam name="T">Type of input data.</typeparam>
    /// <example>
    /// <code>
    /// </code>
    /// </example>
    public class DbMerge<T> :
        DataFlowTransformation<T, T>,
        IDataFlowBatchDestination<T>,
        IDataFlowTransformation<T, T>
        where T : class, IMergeableRow
    {
        public DbMerge(
            TableDefinition tableDefinition,
            IConnectionManager connectionManager = null,
            int batchSize = DbDestination.DefaultBatchSize,
            Func<T> createItem = null
            )
        {
            TableDefinition = tableDefinition ?? throw new ArgumentNullException(nameof(tableDefinition));
            tableDefinition.ValidateName(nameof(tableDefinition));
            destinationTableAsSource = new DbSource<T>(tableDefinition, connectionManager, createItem);
            destinationTable = new DbDestination<T>(tableDefinition, connectionManager, batchSize: batchSize);
            ConnectionManager = connectionManager;
            InitInternalFlow();
            InitOutputFlow();
        }

        /* ITask Interface */
        public override string TaskName { get; set; } = "Insert, Upsert or delete in destination";

        #region IdColumnNames

        public IReadOnlyList<string> IdColumnNames { get; private set; }

        private void SetIdColumnNames()
        {
            if (IdColumnNames != null)
                return;
            var idColumnNames = destinationTable.TypeInfo.IsDynamic ?
                DynamicMergeableRow.GetIdPropertyNames(TableDefinition) :
                MergeableRowProperties.Of(GetType()).IdColumnNames.Select(i => i.columnName);
            IdColumnNames = idColumnNames.ToArray();
        }

        #endregion

        public async Task ExecuteAsync() => await outputSource.ExecuteAsync();
        public void Execute() => outputSource.Execute();

        /* Public Properties */
        public override ISourceBlock<T> SourceBlock => outputSource.SourceBlock;
        public override ITargetBlock<T> TargetBlock => lookup.TargetBlock;
        public DeltaMode DeltaMode { get; set; }

        protected readonly TableDefinition TableDefinition;

        public override IConnectionManager ConnectionManager
        {
            get => base.ConnectionManager;
            set
            {
                base.ConnectionManager = value;
                destinationTableAsSource.ConnectionManager = value;
                destinationTable.ConnectionManager = value;
            }
        }
        public List<T> DeltaTable { get; set; } = new List<T>();
        public bool UseTruncateMethod
        {
            get => IdColumnNames.Count == 0 || useTruncateMethod;
            set => useTruncateMethod = value;
        }
        public int BatchSize
        {
            get => destinationTable.BatchSize;
            set => destinationTable.BatchSize = value;
        }

        /* Private stuff */
        ObjectNameDescriptor TableName => new ObjectNameDescriptor(TableDefinition.Name, ConnectionType);
        List<T> OriginalDestinationRows => lookup.LookupData;

        bool useTruncateMethod;
        LookupTransformation<T, T> lookup;
        readonly DbSource<T> destinationTableAsSource;
        readonly DbDestination<T> destinationTable;
        Dictionary<string, T> destinationIdToOriginalRow;
        CustomSource<T> outputSource;
        bool wasTruncationExecuted;

        private void InitInternalFlow()
        {
            lookup = new LookupTransformation<T, T>(
                destinationTableAsSource,
                row => SetChangeAction(row)
                );

            destinationTable.BeforeBatchWrite = batch =>
            {
                SetIdColumnNames();
                if (DeltaMode == DeltaMode.Delta)
                    DeltaTable.AddRange(batch.WithoutChangeAction(ChangeAction.Delete));
                else
                    DeltaTable.AddRange(batch);

                if (UseTruncateMethod)
                {
                    TruncateDestinationOnce();
                    return batch.WithChangeAction(
                        ChangeAction.Insert,
                        ChangeAction.Update,
                        ChangeAction.None
                        ).
                        ToArray();
                }
                else
                {
                    SqlDeleteIds(batch.WithoutChangeAction(
                        ChangeAction.Insert,
                        ChangeAction.None
                        ));
                    return batch.WithChangeAction(
                        ChangeAction.Insert,
                        ChangeAction.Update
                        ).
                        ToArray();
                }
            };

            lookup.LinkTo(destinationTable);
        }

        private void InitOutputFlow()
        {
            int x = 0;
            outputSource = new CustomSource<T>(
                () => DeltaTable[x++],
                () => x >= DeltaTable.Count
                );

            destinationTable.OnCompletion = () =>
            {
                SetIdColumnNames();
                IdentifyAndDeleteMissingEntries();
                outputSource.Execute();
            };
        }

        private T SetChangeAction(T row)
        {
            if (row is null)
                throw new ArgumentNullException(nameof(row));
            row.SetChangeAction();
            InitDestinationIdToOriginalRowOnce();
            destinationIdToOriginalRow.TryGetValue(row.Id, out var originalDestinationRow);
            if (row.ChangeAction.HasValue)
            {
                if (originalDestinationRow != null)
                    originalDestinationRow.ChangeAction = row.ChangeAction;
            }
            else
            {
                if (originalDestinationRow is null)
                    row.ChangeAction = ChangeAction.Insert;
                else
                {
                    if (row.EqualsWithoutId(originalDestinationRow))
                    {
                        row.ChangeAction = ChangeAction.None;
                        originalDestinationRow.ChangeAction = ChangeAction.None;
                    }
                    else
                    {
                        row.ChangeAction = ChangeAction.Update;
                        originalDestinationRow.ChangeAction = ChangeAction.Update;
                    }
                }
            }
            return row;
        }

        private void InitDestinationIdToOriginalRowOnce()
        {
            if (destinationIdToOriginalRow != null)
                return;
            destinationIdToOriginalRow = new Dictionary<string, T>();
            foreach (var d in OriginalDestinationRows)
                destinationIdToOriginalRow.Add(d.Id, d);
        }

        void TruncateDestinationOnce()
        {
            if (
                wasTruncationExecuted ||
                DeltaMode == DeltaMode.NoDeletions
                )
            {
                return;
            }
            wasTruncationExecuted = true;
            TruncateTableTask.Truncate(ConnectionManager, TableDefinition.Name);
        }

        void IdentifyAndDeleteMissingEntries()
        {
            if (DeltaMode == DeltaMode.NoDeletions)
                return;
            IEnumerable<T> deletions = null;
            if (DeltaMode == DeltaMode.Delta)
                deletions = OriginalDestinationRows.WithChangeAction(ChangeAction.Delete).ToArray();
            else
                deletions = OriginalDestinationRows.WithChangeAction(null).ToArray();
            if (!UseTruncateMethod)
                SqlDeleteIds(deletions);
            foreach (var row in deletions)
                row.ChangeAction = ChangeAction.Delete;
            DeltaTable.AddRange(deletions);
        }

        private void SqlDeleteIds(IEnumerable<T> rowsToDelete)
        {
            if (IdColumnNames.Count == 0)
                throw new InvalidOperationException();
            if (!rowsToDelete.Any())
                return;
            var idsToDelete = rowsToDelete.Select(row => $"'{row.Id}'");
            string id = ConnectionType.ConcatColumns(IdColumnNames);
            foreach (var idsToDeleteBatch in idsToDelete.Batch(BatchSize))
                SqlDeleteIds(id, idsToDelete);
        }

        private void SqlDeleteIds(string id, IEnumerable<string> idsToDelete)
        {
            string ids = string.Join(",", idsToDelete);
            new SqlTask(this, $@"DELETE FROM {TableName.QuotatedFullName} WHERE {id} IN ({ids})")
            {
                DisableLogging = true
            }.
            ExecuteNonQuery();
        }

        public void Wait() => destinationTable.Wait();
        public Task Completion => destinationTable.Completion;
    }
}
