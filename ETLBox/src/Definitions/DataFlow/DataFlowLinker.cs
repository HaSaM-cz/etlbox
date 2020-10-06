using System;
using System.Threading.Tasks.Dataflow;
using CF = ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.DataFlow
{
    public class DataFlowLinker<TOutput>
    {
        public ISourceBlock<TOutput> SourceBlock { get; set; }
        public bool DisableLogging => CallingTask.DisableLogging;
        public NLog.Logger NLogger { get; set; } = CF.ControlFlow.GetLogger();
        public DataFlowTask CallingTask { get; set; }

        public DataFlowLinker(DataFlowTask callingTask, ISourceBlock<TOutput> sourceBlock)
        {
            this.CallingTask = callingTask ?? throw new ArgumentNullException(nameof(callingTask));
            this.SourceBlock = sourceBlock ?? throw new ArgumentNullException(nameof(sourceBlock));
        }

        public (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target)
            => LinkTo<TOutput>(target);

        public (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
        {
            var link = SourceBlock.LinkTo(target.TargetBlock);
            target.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                NLogger.Debug(CallingTask.TaskName + $" was linked to: {((ITask)target).TaskName}", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            return (link, target as IDataFlowLinkSource<TConvert>);
        }

        public (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => LinkTo<TOutput>(target, predicate);

        public (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
        {
            var link = SourceBlock.LinkTo(target.TargetBlock, predicate);
            target.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                NLogger.Debug(CallingTask.TaskName + $" was linked to (with predicate): {((ITask)target).TaskName}!", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            return (link, target as IDataFlowLinkSource<TConvert>);
        }

        public (IDisposable link, IDataFlowLinkSource<TOutput> source) LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => LinkTo<TOutput>(target, rowsToKeep, rowsIntoVoid);

        public (IDisposable link, IDataFlowLinkSource<TConvert> source) LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
        {
            var link = SourceBlock.LinkTo(target.TargetBlock, rowsToKeep);
            target.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                NLogger.Debug(CallingTask.TaskName + $" was linked to (with predicate): {((ITask)target).TaskName}!", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);

            VoidDestination<TOutput> voidTarget = new VoidDestination<TOutput>();
            SourceBlock.LinkTo<TOutput>(voidTarget.TargetBlock, rowsIntoVoid);
            voidTarget.AddPredecessorCompletion(SourceBlock.Completion);
            if (!DisableLogging)
                NLogger.Debug(CallingTask.TaskName + $" was also linked to: VoidDestination to ignore certain rows!", CallingTask.TaskType, "LOG", CallingTask.TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);

            return (link, target as IDataFlowLinkSource<TConvert>);
        }
    }
}
