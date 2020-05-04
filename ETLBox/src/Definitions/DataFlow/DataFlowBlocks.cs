using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// <see cref="IDataflowBlock"/> helper
    /// </summary>
    public static class DataFlowBlocks
    {
        public static readonly DataflowLinkOptions LinkOptionsWithCompletionPropagation =
            new DataflowLinkOptions { PropagateCompletion = true };

        public static IDisposable LinkToWithCompletionPropagation<T>(this ISourceBlock<T> source, ITargetBlock<T> target)
        {
            if (source is null)
                throw new ArgumentNullException(nameof(source));
            if (target is null)
                throw new ArgumentNullException(nameof(target));
            return source.LinkTo(target, LinkOptionsWithCompletionPropagation);
        }
    }
}
