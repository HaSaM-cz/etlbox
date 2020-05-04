using ALE.ETLBox.ConnectionManager;
using System;

namespace ALE.ETLBox
{
    public abstract class DataFlowTask :
        GenericTask,
        ITask,
        IDataFlowProgress
    {
        protected DataFlowTask(IConnectionManager connectionManager = null) :
            base(connectionManager)
        { }

        protected ulong? _loggingThresholdRows;

        public virtual ulong? LoggingThresholdRows
        {
            get
            {
                if (DataFlow.DataFlow.HasLoggingThresholdRows)
                    return DataFlow.DataFlow.LoggingThresholdRows;
                else
                    return _loggingThresholdRows;
            }
            set
            {
                _loggingThresholdRows = value;
            }
        }

        #region Progress

        public ulong ProgressCount
        {
            get => progressCount;
            protected set
            {
                if (progressCount == value)
                    return;
                progressCount = value;
                OnProgressCountChanged(value);
            }
        }
        public event EventHandler<ulong> ProgressCountChanged;

        protected virtual void OnProgressCountChanged(ulong value) => ProgressCountChanged?.Invoke(this, value);

        private ulong progressCount;

        #endregion

        protected bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
        protected ulong ThresholdCount { get; set; } = 1;


        protected void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        protected void LogProgressBatch(ulong rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
                ThresholdCount++;
            }
        }

        protected void LogProgress()
        {
            ProgressCount += 1;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }


    }

}
