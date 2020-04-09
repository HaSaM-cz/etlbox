using System;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Helps with item creation
    /// </summary>
    /// <typeparam name="T">Item type</typeparam>
    public static class ItemFactory<T>
    {
        /// <summary>
        /// Creates new item with <see cref="Activator.CreateInstance{T}"/>
        /// </summary>
        public static T CreateDefault() => Activator.CreateInstance<T>();
        /// <summary>
        /// Creates new array with <see cref="Activator.CreateInstance(Type, object[])"/>
        /// </summary>
        /// <param name="length">Array length</param>
        /// <exception cref="ArgumentOutOfRangeException"><typeparamref name="T"/> is not an array</exception>
        public static T CreateArray(int length)
        {
            if (!typeof(T).IsArray)
                throw new ArgumentOutOfRangeException(nameof(T), typeof(T), "Type must be an array");
            return (T)Activator.CreateInstance(typeof(T), length);
        }

        /// <summary>
        /// Gets <see cref="CreateArray(int)"/> if <typeparamref name="T"/> is an array, otherwise <see cref="CreateDefault"/> as function
        /// </summary>
        /// <param name="length">Array length</param>
        public static Func<T> CreateDefaultOrArray(int length) => typeof(T).IsArray ?
            () => CreateArray(length) :
            (Func<T>)CreateDefault;
    }
}
