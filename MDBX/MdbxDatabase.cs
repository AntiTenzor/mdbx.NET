﻿using System;
using System.Runtime.InteropServices;
using System.Text;

namespace MDBX
{
    using Interop;

    /// <summary>
    /// A table handle denotes the name and parameters of a table, independently
    /// of whether such a table exists.The table handle may be discarded by
    /// calling mdbx_dbi_close(). The old table handle is returned if the table
    /// was already open.The handle may only be closed once.
    /// 
    /// The table handle will be private to the current transaction until
    /// the transaction is successfully committed.If the transaction is
    /// aborted the handle will be closed automatically.
    /// After a successful commit the handle will reside in the shared
    /// environment, and may be used by other transactions.
    /// </summary>
    public class MdbxDatabase : IDisposable
    {

        private readonly MdbxEnvironment _env;
        private readonly MdbxTransaction _tran;
        private readonly uint _dbi;

        internal MdbxDatabase(MdbxEnvironment env, MdbxTransaction tran, uint dbi)
        {
            _env = env;
            _tran = tran;
            _dbi = dbi;
        }

        /// <summary>
        /// Close a database handle. Normally unnecessary.
        /// Closing a database handle is not necessary, but lets mdbx_dbi_open()
        /// reuse the handle value.  Usually it's better to set a bigger
        /// mdbx_env_set_maxdbs(), unless that value would be large.
        /// </summary>
        public void Close()
        {
            Dbi.Close(_env._envPtr, _dbi);
        }


        /// <summary>
        /// Drop this database
        /// </summary>
        public void Drop()
        {
            Dbi.Drop(_tran._txnPtr, _dbi, true);
        }

        /// <summary>
        /// delete all keys in this database to empty it
        /// </summary>
        public void Empty()
        {
            Dbi.Drop(_tran._txnPtr, _dbi, false);
        }

        /// <summary>
        /// Put data into a database.
        /// </summary>
        /// <param name="key">A key to identify this value (NOT-NULL and NOT-EMPTY).</param>
        /// <param name="value">A byte array containing the value to put in the database.</param>
        /// <param name="option">Operation options (optional).</param>
        public unsafe void Put(byte[] key, byte[] value, PutOption option = PutOption.Unspecific)
        {
            fixed (byte* keyPtr = key)
            fixed (byte* valuePtr = value)
            {
                DbValue dbKey = new DbValue((IntPtr)keyPtr, key.Length);
                DbValue dbValue = new DbValue((IntPtr)valuePtr, value.Length);

                Dbi.Put(_tran._txnPtr, _dbi, dbKey, dbValue, option);
            }
        }

        /// <summary>
        /// Put data into a database for an integer key.
        /// </summary>
        /// <param name="key">An integer key to identify this value.</param>
        /// <param name="value">A byte array containing the value to put in the database.</param>
        /// <param name="option">Operation options (optional).</param>
        public unsafe void Put(int key, byte[] value, PutOption option = PutOption.Unspecific)
        {
            byte* keyPtr = (byte*)(&key);
            fixed (byte* valuePtr = value)
            {
                DbValue dbKey = new DbValue((IntPtr)keyPtr, sizeof(int));
                DbValue dbValue = new DbValue((IntPtr)valuePtr, value.Length);

                Dbi.Put(_tran._txnPtr, _dbi, dbKey, dbValue, option);
            }
        }

        /// <summary>
        /// Put string data into a database for an integer key.
        /// </summary>
        /// <param name="key">An integer key to identify this value.</param>
        /// <param name="text">A string containing the value to put in the database.</param>
        /// <param name="encoding">Encoding to convert string to byte array (NOT NULL!).</param>
        /// <param name="option">Operation options (optional).</param>
        public unsafe void Put(int key, string text, Encoding encoding, PutOption option = PutOption.Unspecific)
        {
            if (encoding == null)
                throw new ArgumentNullException(nameof(encoding));

            // It is better to throw ANEx when (value==null), than to replace it with empty array.
            if (text == null)
                throw new ArgumentNullException(nameof(text));

            byte[] value = encoding.GetBytes(text);
            byte* keyPtr = (byte*)(&key);
            fixed (byte* valuePtr = value)
            {
                DbValue dbKey = new DbValue((IntPtr)keyPtr, sizeof(int));
                DbValue dbValue = new DbValue((IntPtr)valuePtr, value.Length);

                Dbi.Put(_tran._txnPtr, _dbi, dbKey, dbValue, option);
            }
        }

        public void Put<K, V>(K key, V value, PutOption option = PutOption.Unspecific)
        {
            ISerializer<K> keySerializer = SerializerRegistry.Get<K>();
            ISerializer<V> valueSerializer = SerializerRegistry.Get<V>();
            Put(keySerializer.Serialize(key), valueSerializer.Serialize(value), option);
        }


        /// <summary>
        /// Get byte array value for a single key
        /// </summary>
        /// <param name="key">byte array key (NOT NULL)</param>
        /// <returns>null if key is not found</returns>
        public unsafe byte[] Get(byte[] key)
        {
            try
            {
                fixed (byte* keyPtr = key)
                {
                    DbValue dbKey = new DbValue((IntPtr)keyPtr, key.Length);
                    DbValue dbValue = Dbi.Get(_tran._txnPtr, _dbi, dbKey);

                    byte[] buffer = null;
                    if (dbValue.Address != IntPtr.Zero && dbValue.Length >= 0)
                    {
                        buffer = new byte[dbValue.Length];
                        if (dbValue.Length > 0)
                        {
                            Marshal.Copy(dbValue.Address, buffer, 0, buffer.Length);
                        }
                    }

                    return buffer;
                }
            }
            catch (MdbxException ex)
            {
                if (ex.ErrorNumber == MdbxCode.MDBX_NOTFOUND)
                    return null; // key not found
                throw;
            }
        }

        /// <summary>
        /// Get byte array value for a single integer key
        /// </summary>
        /// <param name="key">integer key</param>
        /// <returns>null if key is not found</returns>
        public unsafe byte[] Get(int key)
        {
            try
            {
                byte* keyPtr = (byte*)(&key);
                DbValue dbKey = new DbValue((IntPtr)keyPtr, sizeof(int));
                DbValue dbValue = Dbi.Get(_tran._txnPtr, _dbi, dbKey);

                byte[] buffer = null;
                if (dbValue.Address != IntPtr.Zero && dbValue.Length >= 0)
                {
                    buffer = new byte[dbValue.Length];
                    if (dbValue.Length > 0)
                    {
                        Marshal.Copy(dbValue.Address, buffer, 0, buffer.Length);
                    }
                }

                return buffer;
            }
            catch (MdbxException ex)
            {
                if (ex.ErrorNumber == MdbxCode.MDBX_NOTFOUND)
                    return null; // key not found

                throw;
            }
        }

        /// <summary>
        /// Get string value for a single integer key
        /// </summary>
        /// <param name="key">integer key</param>
        /// <param name="encoding">Encoding to convert byte array to string (NOT NULL!).</param>
        /// <returns>null if key is not found</returns>
        public unsafe string Get(int key, Encoding encoding)
        {
            if (encoding == null)
                throw new ArgumentNullException(nameof(encoding));

            try
            {
                byte* keyPtr = (byte*)(&key);
                DbValue dbKey = new DbValue((IntPtr)keyPtr, sizeof(int));
                DbValue dbValue = Dbi.Get(_tran._txnPtr, _dbi, dbKey);

                if (dbValue.Address == IntPtr.Zero)
                    return null;

                if (dbValue.Length > 0)
                {
                    string res = encoding.GetString((byte*)dbValue.Address.ToPointer(), dbValue.Length);
                    return res;
                }
                else if (dbValue.Length == 0)
                {
                    return String.Empty;
                }
                else
                {
                    // TODO: is it possible?
                    //if (dbValue.Length < 0)
                    
                    return null;
                }
            }
            catch (MdbxException ex)
            {
                if (ex.ErrorNumber == MdbxCode.MDBX_NOTFOUND)
                    return null; // key not found

                throw;
            }
        }

        /// <summary>
        /// Get string value for a single integer key.
        /// This is soft implementation, that just returns error code in case of problems.
        /// </summary>
        /// <param name="key">integer key</param>
        /// <param name="encoding">Encoding to convert byte array to string (NOT NULL!).</param>
        /// <param name="text">result string value</param>
        /// <returns>result code</returns>
        /// <exception cref="ArgumentNullException">if encoding is null</exception>
        public unsafe int Get(int key, Encoding encoding, out string text)
        {
            if (encoding == null)
                throw new ArgumentNullException(nameof(encoding));

            text = null;

            try
            {
                byte* keyPtr = (byte*)(&key);
                DbValue dbKey = new DbValue((IntPtr)keyPtr, sizeof(int));
                int errCode = Dbi.Get(_tran._txnPtr, _dbi, dbKey, out DbValue dbValue);
                if (errCode != MdbxCode.MDBX_SUCCESS)
                    return errCode;

                if (dbValue.Address == IntPtr.Zero)
                    return MdbxCode.MDBX_NOTFOUND;

                if (dbValue.Length > 0)
                {
                    text = encoding.GetString((byte*)dbValue.Address.ToPointer(), dbValue.Length);
                    return MdbxCode.MDBX_SUCCESS;
                }
                else if (dbValue.Length == 0)
                {
                    text = String.Empty;
                    return MdbxCode.MDBX_SUCCESS;
                }
                else
                {
                    // TODO: is it possible?
                    //if (dbValue.Length < 0)

                    return MdbxCode.MDBX_NOTFOUND;
                }
            }
            catch (MdbxException ex)
            {
                if (ex.ErrorNumber == MdbxCode.MDBX_NOTFOUND)
                    return ex.ErrorNumber; // key not found

                //throw;

                // In this method I'll return all errors
                return ex.ErrorNumber;
            }
        }

        /// <summary>
        /// Get a single key
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public V Get<K, V>(K key)
        {
            ISerializer<K> keySerializer = SerializerRegistry.Get<K>();
            ISerializer<V> valueSerializer = SerializerRegistry.Get<V>();
            byte[] buffer = Get(keySerializer.Serialize(key));
            if (buffer == null)
                return default(V);
            return valueSerializer.Deserialize(buffer);
        }

        /// <summary>
        /// Get a single key
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <param name="key"></param>
        /// <returns></returns>
        public byte[] Get<K>(K key)
        {
            return Get<K, byte[]>(key);
        }

        /// <summary>
        /// Delete a specific key
        /// </summary>
        /// <param name="key"></param>
        /// <returns>true if deleted successfully; false means not-found</returns>
        public bool Del(byte[] key)
        {
            IntPtr keyPtr = Marshal.AllocHGlobal(key.Length);

            try
            {
                Marshal.Copy(key, 0, keyPtr, key.Length);

                DbValue dbKey = new DbValue(keyPtr, key.Length);
                Dbi.Del(_tran._txnPtr, _dbi, dbKey, IntPtr.Zero);

                return true;
            }
            catch (MdbxException ex)
            {
                if (ex.ErrorNumber == MdbxCode.MDBX_NOTFOUND)
                    return false; // key not found
                throw;
            }
            finally
            {
                Marshal.FreeHGlobal(keyPtr);
            }
        }

        /// <summary>
        /// Delete a specific key
        /// </summary>
        /// <param name="key"></param>
        /// <returns>true if deleted successfully; false means not-found</returns>
        public bool Del<K>(K key)
        {
            ISerializer<K> keySerializer = SerializerRegistry.Get<K>();
            return Del(keySerializer.Serialize(key));
        }


        /// <summary>
        /// Create a cursor handle.
        /// 
        /// A cursor is associated with a specific transaction and database.
        /// A cursor cannot be used when its database handle is closed.  Nor
        /// when its transaction has ended, except with mdbx_cursor_renew().
        /// It can be discarded with mdbx_cursor_close().
        /// 
        /// A cursor must be closed explicitly always, before
        /// or after its transaction ends. It can be reused with
        /// mdbx_cursor_renew() before finally closing it.
        /// </summary>
        /// <returns></returns>
        public MdbxCursor OpenCursor()
        {
            IntPtr ptr = Cursor.Open(_tran._txnPtr, _dbi);
            return new MdbxCursor(_env, _tran, this, ptr);
        }

        #region Implements IDisposable
        /// <inheritdoc/>
        public void Dispose()
        {
            Close();
        }
        #endregion Implements IDisposable
    }
}
