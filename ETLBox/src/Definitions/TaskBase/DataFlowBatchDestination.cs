using ALE.ETLBox.ConnectionManager;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> :
        DataFlowDestination<TInput[]>,
        IDataFlowBatchDestination<TInput>,
        IDisposable
    {
        protected DataFlowBatchDestination(IConnectionManager connectionManager = null, int batchSize = DefaultBatchSize) :
            base(connectionManager)
            => BatchSize = batchSize;

        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }
        public new ITargetBlock<TInput> TargetBlock => Buffer;

        #region BatchSize

        public const int DefaultBatchSize = 1000;

        /// <summary>
        /// <see cref="IDataFlowBatchDestination{TInput}.BatchSize"/>
        /// </summary>
        public int BatchSize
        {
            get => batchSize;
            set
            {
                InitObjects(value);
                batchSize = value;
            }
        }

        private int batchSize;

        #endregion

        public new void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(t => CheckCompleteAction());
        }

        protected new void CheckCompleteAction()
        {
            Task.WhenAll(PredecessorCompletions).ContinueWith(t =>
            {
                if (!TargetBlock.Completion.IsCompleted)
                {
                    if (t.IsFaulted) TargetBlock.Fault(t.Exception.InnerException);
                    else TargetBlock.Complete();
                }
            });
        }

        protected BatchBlock<TInput> Buffer { get; set; }

        protected virtual void InitObjects(int batchSize)
        {
            bufferToTargetActionLink?.Dispose();
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(data =>
            {
                if (ProgressCount == 0)
                    NLogStart();
                if (data.Length == 0)
                    return;
                WriteBatch(ref data);
            });
            SetCompletionTask();
            bufferToTargetActionLink = Buffer.LinkToWithCompletionPropagation(TargetAction);
        }

        protected virtual void WriteBatch(ref TInput[] data)
        {
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
        }

        public virtual void Dispose() => bufferToTargetActionLink?.Dispose();

        private IDisposable bufferToTargetActionLink;
    }
}
