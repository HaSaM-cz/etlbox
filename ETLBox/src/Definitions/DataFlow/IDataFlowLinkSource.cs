using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowLinkSource<TOutput>
    {
        ISourceBlock<TOutput> SourceBlock { get; }

        (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target);
        (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate);
        (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);

        (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target);
        (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate);
        (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid);

    }
}
