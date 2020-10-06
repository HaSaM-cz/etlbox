﻿using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public class ErrorHandler
    {
        public ISourceBlock<ETLBoxError> ErrorSourceBlock => ErrorBuffer;
        internal BufferBlock<ETLBoxError> ErrorBuffer { get; set; }
        internal bool HasErrorBuffer => ErrorBuffer != null;


        public IDisposable LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target, Task completion)
        {
            ErrorBuffer = new BufferBlock<ETLBoxError>();
            var link = ErrorSourceBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions());
            target.AddPredecessorCompletion(ErrorSourceBlock.Completion);
            completion.ContinueWith(t => ErrorBuffer.Complete());
            return link;
        }

        public void Send(Exception e, string jsonRow)
        {
            ErrorBuffer.SendAsync(new ETLBoxError()
            {
                Exception = e,
                ErrorText = e.Message,
                ReportTime = DateTime.Now,
                RecordAsJson = jsonRow
            }).Wait();
        }

        public static string ConvertErrorData<T>(T row)
        {
            try
            {
                return JsonConvert.SerializeObject(row, new JsonSerializerSettings());
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
    }
}
