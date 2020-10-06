using ALE.ETLBox.ConnectionManager;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowSource<TOutput> : DataFlowTask, ITask
    {
        protected DataFlowSource(IConnectionManager connectionManager = null) :
            base(connectionManager)
        { }

        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        protected BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();

        protected ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public abstract void Execute();

        public Task ExecuteAsync()
        {
            return Task.Factory.StartNew(
                () => Execute()
                );
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

        public IDisposable LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, SourceBlock.Completion);

    }
}
