﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// This attribute defines if the column is included in the comparison to identify
    /// object that exists and needs to be updated. It it supposed
    /// to use with an object that inherits from <see cref="MergeableRow"/>.
    /// If you implement the <see cref="IMergeableRow"/> interface, you need to override the <see cref="object.Equals"/> method instead.
    /// </summary>
    /// <example>
    ///  public class MyPoco : MergeableRow
    /// {
    ///     [IdColumn]
    ///     public int Key { get; set; }
    ///     [CompareColumn]
    ///     public string Value {get;set; }
    /// }
    /// </example>
    [AttributeUsage(AttributeTargets.Property)]
    public class CompareColumn : Attribute
    {
        public CompareColumn()
        {
        }
    }
}
