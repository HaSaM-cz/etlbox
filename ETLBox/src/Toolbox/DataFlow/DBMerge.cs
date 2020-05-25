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
            MergeMode mode,
            TableDefinition tableDefinition,
            IConnectionManager connectionManager = null,
            int batchSize = DbDestination.DefaultBatchSize,
            Func<T> createItem = null
            )
        {
            Mode = mode;
            TableDefinition = tableDefinition ?? throw new ArgumentNullException(nameof(tableDefinition));
            tableDefinition.ValidateName(nameof(tableDefinition));
            this.createItem = createItem;
            destinationTable = new DbDestination<T>(tableDefinition, connectionManager, batchSize: batchSize);
            ConnectionManager = connectionManager;
            InitInternalFlow();
            InitOutputFlow();
        }

        /* ITask Interface */
        public override string TaskName { get; set; } = "Insert, Upsert or delete in destination";

        #region IdColumnNames

        /// <summary>
        /// Names of destination table columns comprising <see cref="IMergeableRow.Id"/>
        /// </summary>
        /// <value>Available during or after merging process, otherwise null</value>
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

        public MergeMode Mode { get; }

        public override ITargetBlock<T> TargetBlock => fullMergeTarget?.TargetBlock ?? partialMergeTarget;
        public override ISourceBlock<T> SourceBlock => outputSource.SourceBlock;

        protected readonly TableDefinition TableDefinition;

        public override IConnectionManager ConnectionManager
        {
            get => base.ConnectionManager;
            set
            {
                base.ConnectionManager = value;
                if (fullMergeTarget != null)
                    ((DbSource<T>)fullMergeTarget.Source).ConnectionManager = value;
                destinationTable.ConnectionManager = value;
            }
        }
        public List<T> DeltaTable { get; set; } = new List<T>();
        /// <summary>
        /// Whether to truncate destination table before merging changes into it
        /// </summary>
        /// <remarks>Applicable only in <see cref="MergeMode.Full"/> <see cref="Mode"/> and with any <see cref="IdColumnNames"/></remarks>
        /// <value>Default: false</value>
        public bool UseTruncateMethod
        {
            get =>
                Mode == MergeMode.Full &&
                (
                    IdColumnNames?.Count == 0 ||
                    useTruncateMethod
                );
            set => useTruncateMethod = value;
        }
        public int BatchSize
        {
            get => destinationTable.BatchSize;
            set => destinationTable.BatchSize = value;
        }

        private ObjectNameDescriptor TableName => new ObjectNameDescriptor(TableDefinition.Name, ConnectionType);
        private IList<T> originalDestinationRows;

        private LookupTransformation<T, T> fullMergeTarget;
        private BatchBlock<T> partialMergeTarget;
        private readonly Func<T> createItem;
        private readonly DbDestination<T> destinationTable;
        private Dictionary<string, T> destinationIdToOriginalRow;
        private bool useTruncateMethod;
        private bool wasTruncationExecuted;
        private CustomSource<T> outputSource;

        private void InitInternalFlow()
        {
            destinationTable.BeforeBatchWrite = BeforeBatchWrite;
            // full merge
            if (Mode == MergeMode.Full)
            {
                var destinationTableAsSource = new DbSource<T>(TableDefinition, ConnectionManager, createItem);
                fullMergeTarget = new LookupTransformation<T, T>(destinationTableAsSource, SetChangeAction);
                originalDestinationRows = fullMergeTarget.LookupData;
                fullMergeTarget.LinkTo(destinationTable);
            }
            // partial merge
            else
            {
                partialMergeTarget = new BatchBlock<T>(destinationTable.BatchSize);
                var sourceBatchProcessor = new TransformManyBlock<T[], T>(ProcessSourceBatch);
                partialMergeTarget.LinkToWithCompletionPropagation(sourceBatchProcessor);
                sourceBatchProcessor.LinkToWithCompletionPropagation(destinationTable.TargetBlock);
            }
        }

        private T[] BeforeBatchWrite(T[] batch)
        {
            SetIdColumnNames();
            DeltaTable.AddRange(Mode == MergeMode.NoDeletions ?
                batch.WithoutChangeAction(ChangeAction.Delete) :
                batch
                );
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
                SqlDelete(batch.WithoutChangeAction(
                    ChangeAction.Insert,
                    ChangeAction.None
                    ));
                return batch.WithChangeAction(
                    ChangeAction.Insert,
                    ChangeAction.Update
                    ).
                    ToArray();
            }
        }

        private IEnumerable<T> ProcessSourceBatch(T[] batch)
        {
            originalDestinationRows = GetDestinationRowsForSourceBatch(batch);
            destinationIdToOriginalRow = null;
            foreach (var row in batch)
                SetChangeAction(row);
            return batch;
        }

        private IList<T> GetDestinationRowsForSourceBatch(T[] batch)
        {
            SetIdColumnNames();
            string sql = SqlFromTableWhereIdIn("select *", batch);
            var source = new DbSource<T>(sql, ConnectionManager, createItem);
            var rowsBatcher = new BatchBlock<T>(batch.Length);
            T[] rows = null;
            var batchProcessor = new ActionBlock<T[]>(i =>
            {
                if (rows != null)
                    throw new InvalidOperationException("Rows already set");
                rows = i;
            });
            using var link1 = source.SourceBlock.LinkToWithCompletionPropagation(rowsBatcher);
            using var link2 = rowsBatcher.LinkToWithCompletionPropagation(batchProcessor);
            source.Execute();
            batchProcessor.Completion.Wait();
            return rows ?? Array.Empty<T>();
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
                if (Mode == MergeMode.Full)
                    IdentifyAndDeleteMissingEntries();
                outputSource.Execute();
            };
        }

        private T SetChangeAction(T row)
        {
            if (row is null)
                throw new ArgumentNullException(nameof(row));
            row.SetChangeAction();
            InitDestinationIdToOriginalRow();
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

        private void InitDestinationIdToOriginalRow()
        {
            if (destinationIdToOriginalRow != null)
                return;
            destinationIdToOriginalRow = new Dictionary<string, T>();
            foreach (var d in originalDestinationRows)
                destinationIdToOriginalRow.Add(d.Id, d);
        }

        void TruncateDestinationOnce()
        {
            if (
                wasTruncationExecuted ||
                Mode == MergeMode.NoDeletions
                )
            {
                return;
            }
            TruncateTableTask.Truncate(ConnectionManager, TableDefinition.Name);
            wasTruncationExecuted = true;
        }

        void IdentifyAndDeleteMissingEntries()
        {
            var deletions = originalDestinationRows.WithChangeAction(null).ToArray();
            SetIdColumnNames();
            if (!UseTruncateMethod)
                SqlDelete(deletions);
            foreach (var row in deletions)
                row.ChangeAction = ChangeAction.Delete;
            DeltaTable.AddRange(deletions);
        }

        private void SqlDelete(IEnumerable<T> rows)
        {
            if (!rows.Any())
                return;
            foreach (var rowsToDeleteBatch in rows.Batch(BatchSize))
                SqlDeleteBatch(rowsToDeleteBatch);
        }

        private void SqlDeleteBatch(IEnumerable<T> rows)
        {
            string sql = SqlFromTableWhereIdIn("delete", rows);
            var task = new SqlTask(this, sql)
            {
                DisableLogging = true
            };
            int count = task.ExecuteNonQuery();
            if (count > 0)
                LogProgressBatch((ulong)count);
        }

        private string SqlFromTableWhereIdIn(string prefix, IEnumerable<T> rows)
        {
            if (IdColumnNames.Count == 0)
                throw new InvalidOperationException($"Missing {nameof(IdColumnNames)}");
            var ids = rows.Select(i => i.Id);
            string sql = ConnectionType.SqlIdIn(IdColumnNames, ids);
            sql = $"{prefix} from {TableName.QuotatedFullName} where {sql}";
            return sql;
        }

        public void Wait() => destinationTable.Wait();
        public Task Completion => destinationTable.Completion;
    }
}
