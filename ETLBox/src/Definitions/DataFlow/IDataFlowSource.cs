using System;
using System.Threading.Tasks;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowSource<TOutput> :
        IDataFlowLinkSource<TOutput>,
        IDataFlowProgress
    {
        Task ExecuteAsync();
        void Execute();

        IDisposable LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target);
    }
}
