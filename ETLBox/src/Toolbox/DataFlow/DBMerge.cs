﻿using ALE.ETLBox.ConnectionManager;
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
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <example>
    /// <code>
    /// </code>
    /// </example>
    public class DbMerge<TInput> :
        DataFlowTransformation<TInput, TInput>,
        IDataFlowBatchDestination<TInput>,
        IDataFlowTransformation<TInput, TInput>
        where TInput : class, IMergeableRow, new()
    {
        public DbMerge(TableDefinition tableDefinition, IConnectionManager connectionManager = null, int batchSize = DbDestination.DefaultBatchSize) :
            base(connectionManager)
        {
            this.TableDefinition = tableDefinition ?? throw new ArgumentNullException(nameof(tableDefinition));
            tableDefinition.ValidateName(nameof(tableDefinition));
            IdColumnNamesForDeletion = new TInput().IdColumnNamesForDeletion.ToArray();
            destinationTableAsSource = new DbSource<TInput>(tableDefinition, connectionManager);
            destinationTable = new DbDestination<TInput>(tableDefinition, connectionManager, batchSize: batchSize);
            InitInternalFlow();
            InitOutputFlow();
        }

        /* ITask Interface */
        public override string TaskName { get; set; } = "Insert, Upsert or delete in destination";
        public IReadOnlyList<string> IdColumnNamesForDeletion { get; }

        public async Task ExecuteAsync() => await outputSource.ExecuteAsync();
        public void Execute() => outputSource.Execute();

        /* Public Properties */
        public override ISourceBlock<TInput> SourceBlock => outputSource.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => lookup.TargetBlock;
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
        public List<TInput> DeltaTable { get; set; } = new List<TInput>();
        public bool UseTruncateMethod
        {
            get => IdColumnNamesForDeletion.Count == 0 || useTruncateMethod;
            set => useTruncateMethod = value;
        }
        public int BatchSize
        {
            get => destinationTable.BatchSize;
            set => destinationTable.BatchSize = value;
        }

        /* Private stuff */
        ObjectNameDescriptor TableName => new ObjectNameDescriptor(TableDefinition.Name, ConnectionType);
        List<TInput> OriginalDestinationRows => lookup.LookupData;

        bool useTruncateMethod;
        LookupTransformation<TInput, TInput> lookup;
        readonly DbSource<TInput> destinationTableAsSource;
        readonly DbDestination<TInput> destinationTable;
        Dictionary<string, TInput> destinationIdToOriginalRow;
        CustomSource<TInput> outputSource;
        bool wasTruncationExecuted;

        private void InitInternalFlow()
        {
            lookup = new LookupTransformation<TInput, TInput>(
                destinationTableAsSource,
                row => UpdateRowWithDeltaInfo(row)
            );

            destinationTable.BeforeBatchWrite = batch =>
            {
                if (DeltaMode == DeltaMode.Delta)
                    DeltaTable.AddRange(batch.Where(row => row.ChangeAction != ChangeAction.Delete));
                else
                    DeltaTable.AddRange(batch);

                if (UseTruncateMethod)
                {
                    TruncateDestinationOnce();
                    return batch.Where(row =>
                        row.ChangeAction == ChangeAction.Insert ||
                        row.ChangeAction == ChangeAction.Update ||
                        row.ChangeAction == ChangeAction.None
                        ).ToArray();
                }
                else
                {
                    SqlDeleteIds(batch.Where(row =>
                        row.ChangeAction != ChangeAction.Insert &&
                        row.ChangeAction != ChangeAction.None
                        ));
                    return batch.Where(row =>
                        row.ChangeAction == ChangeAction.Insert ||
                        row.ChangeAction == ChangeAction.Update
                        ).ToArray();
                }
            };

            lookup.LinkTo(destinationTable);
        }

        private void InitOutputFlow()
        {
            int x = 0;
            outputSource = new CustomSource<TInput>(
                () => DeltaTable[x++],
                () => x >= DeltaTable.Count
                );

            destinationTable.OnCompletion = () =>
            {
                IdentifyAndDeleteMissingEntries();
                outputSource.Execute();
            };
        }

        private TInput UpdateRowWithDeltaInfo(TInput row)
        {
            if (row is null)
                throw new ArgumentNullException(nameof(row));
            row.SetChangeAction();
            InitDestinationIdToOriginalRowOnce();
            destinationIdToOriginalRow.TryGetValue(row.Id, out var originalDestinationRow);
            if (
                DeltaMode == DeltaMode.Delta &&
                row.ChangeAction == ChangeAction.Delete
                )
            {
                if (originalDestinationRow != null)
                    originalDestinationRow.ChangeAction = ChangeAction.Delete;
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
            destinationIdToOriginalRow = new Dictionary<string, TInput>();
            foreach (var d in OriginalDestinationRows)
                destinationIdToOriginalRow.Add(d.Id, d);
        }

        void TruncateDestinationOnce()
        {
            if (wasTruncationExecuted == true) return;
            wasTruncationExecuted = true;
            if (DeltaMode == DeltaMode.NoDeletions == true) return;
            TruncateTableTask.Truncate(this.ConnectionManager, TableDefinition.Name);
        }

        void IdentifyAndDeleteMissingEntries()
        {
            if (DeltaMode == DeltaMode.NoDeletions)
                return;
            IEnumerable<TInput> deletions = null;
            if (DeltaMode == DeltaMode.Delta)
                deletions = OriginalDestinationRows.WithChangeAction(ChangeAction.Delete).ToList();
            else
                deletions = OriginalDestinationRows.WithChangeAction(null).ToList();
            if (!UseTruncateMethod)
                SqlDeleteIds(deletions);
            foreach (var row in deletions)
                row.ChangeAction = ChangeAction.Delete;
            DeltaTable.AddRange(deletions);
        }

        private void SqlDeleteIds(IEnumerable<TInput> rowsToDelete)
        {
            if (IdColumnNamesForDeletion.Count == 0)
                throw new InvalidOperationException();
            if (!rowsToDelete.Any())
                return;
            var idsToDelete = rowsToDelete.Select(row => $"'{row.Id}'");
            string id = ConnectionType.ConcatColumns(IdColumnNamesForDeletion);
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
