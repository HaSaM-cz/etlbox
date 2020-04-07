using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ETLBox.DataFlow
{
    public enum ChangeAction :
        byte
    {
        /// <summary>
        /// Existing record (no change)
        /// </summary>
        None,
        Insert,
        Update,
        Delete
    }
}
