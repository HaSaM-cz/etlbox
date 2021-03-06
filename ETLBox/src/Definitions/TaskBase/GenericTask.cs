﻿using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using CF = ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox
{
    public abstract class GenericTask : ITask
    {
        protected GenericTask(IConnectionManager connectionManager = null)
        {
            if (connectionManager != null)
                ConnectionManager = connectionManager;
        }

        private string _taskType;
        public virtual string TaskType
        {
            get => String.IsNullOrEmpty(_taskType) ? this.GetType().Name : _taskType;
            set => _taskType = value;
        }
        public virtual string TaskName { get; set; } = "N/A";
        public NLog.Logger NLogger { get; protected set; } = CF.ControlFlow.GetLogger();

        public virtual IConnectionManager ConnectionManager { get; set; }

        internal virtual IConnectionManager DbConnectionManager => ConnectionManager.DefaultIfNull();

        public ConnectionManagerType ConnectionType => DbConnectionManager.Type();
        public string QB => ConnectionType.GetBeginQuotation();
        public string QE => ConnectionType.GetEndQuotation();

        public bool _disableLogging;
        public virtual bool DisableLogging
        {
            get => CF.ControlFlow.DisableAllLogging || _disableLogging;
            set => _disableLogging = value;
        }

        private string _taskHash;


        public virtual string TaskHash
        {
            get
            {
                if (_taskHash == null)
                    return HashHelper.Encrypt_Char40(this);
                else
                    return _taskHash;
            }
            set
            {
                _taskHash = value;
            }
        }
        internal virtual bool HasName => !String.IsNullOrWhiteSpace(TaskName);

        public void CopyTaskProperties(ITask otherTask)
        {
            if (otherTask is null)
                throw new ArgumentNullException(nameof(otherTask));
            this.TaskName = otherTask.TaskName;
            this.TaskHash = otherTask.TaskHash;
            this.TaskType = otherTask.TaskType;
            this.ConnectionManager = otherTask.ConnectionManager;
            this.DisableLogging = otherTask.DisableLogging;
        }
    }
}
