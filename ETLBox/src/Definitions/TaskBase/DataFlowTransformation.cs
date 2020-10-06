using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox
{
    public abstract class DataFlowTransformation<TInput, TOutput> : DataFlowTask, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        protected DataFlowTransformation(IConnectionManager connectionManager = null) :
            base(connectionManager)
        { }

        public virtual ITargetBlock<TInput> TargetBlock { get; }
        public virtual ISourceBlock<TOutput> SourceBlock { get; }

        protected List<Task> PredecessorCompletions { get; set; } = new List<Task>();

        public void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(t => CheckCompleteAction());
        }

        protected void CheckCompleteAction()
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

        public (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target)
        => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target);

        public (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, predicate);

        public (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        public (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target);

        public (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, predicate);

        public (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);
    }
}
